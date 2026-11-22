import { randomUUID } from "node:crypto";
import {
  lstat,
  mkdir,
  readFile,
  readdir,
  rename,
  rmdir,
  unlink,
  writeFile,
} from "node:fs/promises";
import path from "node:path";

export interface RuntimeLock {
  path: string;
  release(): Promise<void>;
}

function errorCode(error: unknown): string | undefined {
  return (error as NodeJS.ErrnoException).code;
}

function ownerIsDead(pid: number): boolean {
  if (!Number.isSafeInteger(pid) || pid <= 0) {
    return false;
  }
  try {
    process.kill(pid, 0);
    return false;
  } catch (error) {
    // Permission errors do not prove that the owner has exited.
    return errorCode(error) === "ESRCH";
  }
}

async function removeEmptyDirectory(directory: string): Promise<void> {
  try {
    await rmdir(directory);
  } catch (error) {
    // Another contender may already have published its non-empty directory.
    if (!["ENOENT", "ENOTEMPTY", "EEXIST"].includes(errorCode(error) ?? "")) {
      throw error;
    }
  }
}

async function releaseOwner(directory: string, owner: string): Promise<void> {
  try {
    // The unique filename makes this a conditional delete: a former owner
    // cannot remove the marker belonging to a replacement owner.
    await unlink(path.join(directory, owner));
  } catch (error) {
    if (errorCode(error) === "ENOENT") {
      return;
    }
    throw error;
  }
  // Never recursively remove a lock: the path may now belong to someone else.
  await removeEmptyDirectory(directory);
}

async function canClaim(lockPath: string): Promise<boolean> {
  try {
    const entry = await lstat(lockPath);
    if (entry.isDirectory()) {
      const entries = await readdir(lockPath);
      if (entries.length === 0) {
        // Locks are published non-empty. An empty directory can only be a
        // partially completed release, and rmdir cannot remove a new owner.
        await removeEmptyDirectory(lockPath);
        return true;
      }
      const owner =
        entries.length === 1
          ? /^owner-(\d+)-[0-9a-f-]{36}$/i.exec(entries[0])
          : null;
      if (!owner || !ownerIsDead(Number(owner[1]))) {
        return false;
      }
      await releaseOwner(lockPath, entries[0]);
      return true;
    }
    if (entry.isFile()) {
      // Respect locks created by earlier releases. Only reclaim a complete
      // numeric PID when the operating system confirms it no longer exists.
      const recordedPid = (await readFile(lockPath, "utf8")).trim();
      if (!/^\d+$/.test(recordedPid) || !ownerIsDead(Number(recordedPid))) {
        return false;
      }
      // unlink cannot delete a new directory published by another contender.
      await unlink(lockPath);
      return true;
    }
    return false;
  } catch (error) {
    if (errorCode(error) === "ENOENT") {
      return true;
    }
    if (
      ["EACCES", "EPERM", "EISDIR", "ENOTDIR"].includes(errorCode(error) ?? "")
    ) {
      return false;
    }
    throw error;
  }
}

export async function claimRuntimeLock(
  lockPath: string,
): Promise<RuntimeLock | undefined> {
  if (!(await canClaim(lockPath))) {
    return undefined;
  }

  const token = randomUUID();
  const owner = `owner-${process.pid}-${token}`;
  const preparedPath = `${lockPath}.${process.pid}.${token}.tmp`;
  let published = false;
  try {
    await mkdir(preparedPath, { mode: 0o700 });
    await writeFile(path.join(preparedPath, owner), String(process.pid), {
      flag: "wx",
      mode: 0o600,
    });
    try {
      // A non-empty destination directory cannot be replaced by rename.
      // Publish the PID and ownership token together, with no empty-lock gap.
      await rename(preparedPath, lockPath);
    } catch (error) {
      if (
        ["EEXIST", "ENOTEMPTY", "EISDIR", "ENOTDIR"].includes(
          errorCode(error) ?? "",
        )
      ) {
        return undefined;
      }
      if (["EPERM", "EACCES"].includes(errorCode(error) ?? "")) {
        try {
          await lstat(lockPath);
          return undefined;
        } catch {
          // A genuine permission failure is not a busy runtime slot.
        }
      }
      throw error;
    }
    published = true;
    let releasing: Promise<void> | undefined;
    return {
      path: lockPath,
      release() {
        if (!releasing) {
          releasing = releaseOwner(lockPath, owner).catch((error) => {
            releasing = undefined;
            throw error;
          });
        }
        return releasing;
      },
    };
  } finally {
    if (!published) {
      await unlink(path.join(preparedPath, owner)).catch(() => undefined);
      await rmdir(preparedPath).catch(() => undefined);
    }
  }
}
