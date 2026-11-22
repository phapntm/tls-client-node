import { randomUUID } from "crypto";
import {
  access,
  chmod,
  mkdir,
  readdir,
  rename,
  unlink,
  writeFile,
} from "node:fs/promises";
import path from "path";
import { BinaryInfo, NativeLibraryInfo, TLSClientOptions } from "./types";
import { TLSClientError } from "./errors";

const TLS_CLIENT_API_RELEASES =
  "https://api.github.com/repos/phapntm/tls-client-api/releases";
const TLS_CLIENT_RELEASES =
  "https://api.github.com/repos/bogdanfinn/tls-client/releases";

interface ReleaseAssetSpec {
  releaseBaseUrl: string;
  prefix: string;
  suffix: string;
}

interface ExistingAsset {
  filePath: string;
  version: string;
}

// Shared across clients in this process; completed/failed work is never retained.
const pendingAssets = new Map<string, Promise<ExistingAsset>>();
const pendingDownloads = new Map<string, Promise<void>>();

function sharePending<T>(
  pending: Map<string, Promise<T>>,
  key: string,
  operation: () => Promise<T>,
): Promise<T> {
  const existing = pending.get(key);
  if (existing) {
    return existing;
  }

  const task = Promise.resolve()
    .then(operation)
    .finally(() => {
      pending.delete(key);
    });
  pending.set(key, task);
  return task;
}

function downloadPathKey(filePath: string): string {
  const resolved = path.resolve(filePath);
  return process.platform === "win32" ? resolved.toLowerCase() : resolved;
}

export function supportsNativeRuntime(
  platform = process.platform,
  arch = process.arch,
): boolean {
  return (
    (platform === "win32" && (arch === "x64" || arch === "ia32")) ||
    (platform === "darwin" && (arch === "arm64" || arch === "x64")) ||
    (platform === "linux" && (arch === "x64" || arch === "arm64"))
  );
}

function getApiBinaryAssetSpec(): ReleaseAssetSpec {
  const platform = process.platform;
  const arch = process.arch;

  if (platform === "darwin" && arch === "arm64") {
    return {
      releaseBaseUrl: TLS_CLIENT_API_RELEASES,
      prefix: "tls-client-api-darwin-arm64-",
      suffix: "",
    };
  }

  if (platform === "darwin" && arch === "x64") {
    return {
      releaseBaseUrl: TLS_CLIENT_API_RELEASES,
      prefix: "tls-client-api-darwin-amd64-",
      suffix: "",
    };
  }

  if (platform === "linux" && arch === "x64") {
    return {
      releaseBaseUrl: TLS_CLIENT_API_RELEASES,
      prefix: "tls-client-api-linux-amd64-",
      suffix: "",
    };
  }

  if (platform === "win32" && arch === "x64") {
    return {
      releaseBaseUrl: TLS_CLIENT_API_RELEASES,
      prefix: "tls-client-api-windows-64-",
      suffix: ".exe",
    };
  }

  if (platform === "win32" && arch === "ia32") {
    return {
      releaseBaseUrl: TLS_CLIENT_API_RELEASES,
      prefix: "tls-client-api-windows-32-",
      suffix: ".exe",
    };
  }

  throw new TLSClientError(
    `Unsupported platform for tls-client-api: ${platform}/${arch}`,
    { code: "ERR_UNSUPPORTED_PLATFORM" },
  );
}

function getNativeLibraryAssetSpec(): ReleaseAssetSpec {
  const platform = process.platform;
  const arch = process.arch;

  if (platform === "darwin" && arch === "arm64") {
    return {
      releaseBaseUrl: TLS_CLIENT_RELEASES,
      prefix: "tls-client-darwin-arm64-",
      suffix: ".dylib",
    };
  }

  if (platform === "darwin" && arch === "x64") {
    return {
      releaseBaseUrl: TLS_CLIENT_RELEASES,
      prefix: "tls-client-darwin-amd64-",
      suffix: ".dylib",
    };
  }

  if (platform === "linux" && arch === "x64") {
    return {
      releaseBaseUrl: TLS_CLIENT_RELEASES,
      prefix: "tls-client-linux-ubuntu-amd64-",
      suffix: ".so",
    };
  }

  if (platform === "linux" && arch === "arm64") {
    return {
      releaseBaseUrl: TLS_CLIENT_RELEASES,
      prefix: "tls-client-linux-arm64-",
      suffix: ".so",
    };
  }

  if (platform === "win32" && arch === "x64") {
    return {
      releaseBaseUrl: TLS_CLIENT_RELEASES,
      prefix: "tls-client-windows-64-",
      suffix: ".dll",
    };
  }

  if (platform === "win32" && arch === "ia32") {
    return {
      releaseBaseUrl: TLS_CLIENT_RELEASES,
      prefix: "tls-client-windows-32-",
      suffix: ".dll",
    };
  }

  throw new TLSClientError(
    `Unsupported platform for tls-client native library: ${platform}/${arch}`,
    { code: "ERR_UNSUPPORTED_PLATFORM" },
  );
}

function getDefaultDownloadDir(options: TLSClientOptions): string {
  return path.resolve(options.downloadDir ?? path.join(__dirname, "..", "bin"));
}

function normalizeVersion(version: string): string {
  return version.replace(/^v/i, "");
}

function createHeaders(token?: string): Record<string, string> {
  const headers: Record<string, string> = {
    Accept: "application/vnd.github+json",
    "User-Agent": "tls-client-node",
  };

  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  return headers;
}

async function fetchReleaseMetadata(
  releaseBaseUrl: string,
  version: string | undefined,
  token?: string,
): Promise<any> {
  const url =
    version && version !== "latest"
      ? `${releaseBaseUrl}/tags/v${normalizeVersion(version)}`
      : `${releaseBaseUrl}/latest`;

  const response = await globalThis.fetch(url, {
    headers: createHeaders(token),
  });

  if (!response.ok) {
    throw new TLSClientError(
      `Failed to resolve release metadata: ${response.status} ${response.statusText}`,
      { code: "ERR_RELEASE_LOOKUP", status: response.status },
    );
  }

  return response.json();
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function findExistingAsset(
  directory: string,
  assetSpec: ReleaseAssetSpec,
  requestedVersion?: string,
): Promise<ExistingAsset | null> {
  if (!(await fileExists(directory))) {
    return null;
  }

  const entries = await readdir(directory);
  const matches = entries
    .filter(
      (entry) =>
        entry.startsWith(assetSpec.prefix) && entry.endsWith(assetSpec.suffix),
    )
    .sort();

  if (matches.length === 0) {
    return null;
  }

  const normalizedVersion = requestedVersion
    ? normalizeVersion(requestedVersion)
    : undefined;

  const selected = normalizedVersion
    ? matches.find(
        (entry) =>
          entry ===
          `${assetSpec.prefix}${normalizedVersion}${assetSpec.suffix}`,
      )
    : matches[matches.length - 1];

  if (!selected) {
    return null;
  }

  return {
    filePath: path.join(directory, selected),
    version: selected.slice(
      assetSpec.prefix.length,
      selected.length - assetSpec.suffix.length,
    ),
  };
}

async function downloadBinaryAsset(
  assetUrl: string,
  destinationPath: string,
  reuseExisting: boolean,
): Promise<void> {
  return sharePending(
    pendingDownloads,
    downloadPathKey(destinationPath),
    async () => {
      // Another request may have published the same version during metadata lookup.
      if (reuseExisting && (await fileExists(destinationPath))) {
        return;
      }

      const response = await globalThis.fetch(assetUrl, {
        headers: {
          "User-Agent": "tls-client-node",
        },
      });

      if (!response.ok) {
        throw new TLSClientError(
          `Failed to download release asset: ${response.status} ${response.statusText}`,
          { code: "ERR_BINARY_DOWNLOAD", status: response.status },
        );
      }

      // The leading dot also keeps temporary files out of Linux's suffix-less scan.
      const temporaryPath = path.join(
        path.dirname(destinationPath),
        `.${path.basename(destinationPath)}.${randomUUID()}.tmp`,
      );

      try {
        const arrayBuffer = await response.arrayBuffer();
        await writeFile(temporaryPath, Buffer.from(arrayBuffer), {
          flag: "wx",
        });

        if (process.platform !== "win32") {
          await chmod(temporaryPath, 0o755);
        }

        await rename(temporaryPath, destinationPath);
      } finally {
        await unlink(temporaryPath).catch(() => undefined);
      }
    },
  );
}

async function ensureReleaseAsset(
  options: TLSClientOptions,
  assetSpec: ReleaseAssetSpec,
  description: string,
): Promise<ExistingAsset> {
  const directory = getDefaultDownloadDir(options);
  const key = JSON.stringify([
    downloadPathKey(directory),
    assetSpec.releaseBaseUrl,
    assetSpec.prefix,
    assetSpec.suffix,
    options.version === undefined ? null : normalizeVersion(options.version),
    options.githubToken ?? null,
  ]);

  return sharePending(pendingAssets, key, async () => {
    await mkdir(directory, { recursive: true });
    const existing = await findExistingAsset(
      directory,
      assetSpec,
      options.version,
    );
    if (existing) {
      return existing;
    }

    const metadata = await fetchReleaseMetadata(
      assetSpec.releaseBaseUrl,
      options.version,
      options.githubToken,
    );
    const version = normalizeVersion(
      metadata.tag_name ?? options.version ?? "latest",
    );
    const assetName = `${assetSpec.prefix}${version}${assetSpec.suffix}`;
    const asset = metadata.assets?.find(
      (entry: { name: string }) => entry.name === assetName,
    );

    if (!asset?.browser_download_url) {
      throw new TLSClientError(
        `No ${description} release asset found for ${assetName}`,
        { code: "ERR_BINARY_ASSET_MISSING" },
      );
    }

    const destinationPath = path.join(directory, assetName);
    await downloadBinaryAsset(
      asset.browser_download_url,
      destinationPath,
      options.version !== "latest",
    );

    return { filePath: destinationPath, version };
  });
}

export async function ensureBinary(
  options: TLSClientOptions = {},
): Promise<BinaryInfo> {
  if (options.binaryPath) {
    return {
      executablePath: options.binaryPath,
      version: normalizeVersion(options.version ?? "custom"),
    };
  }

  const asset = await ensureReleaseAsset(
    options,
    getApiBinaryAssetSpec(),
    "tls-client-api",
  );
  return { executablePath: asset.filePath, version: asset.version };
}

export async function ensureNativeLibrary(
  options: TLSClientOptions = {},
): Promise<NativeLibraryInfo> {
  if (options.nativeLibraryPath) {
    return {
      libraryPath: options.nativeLibraryPath,
      version: normalizeVersion(options.version ?? "custom"),
    };
  }

  const asset = await ensureReleaseAsset(
    options,
    getNativeLibraryAssetSpec(),
    "tls-client",
  );
  return { libraryPath: asset.filePath, version: asset.version };
}

export async function writeConfigFile(
  directory: string,
  port: number,
  healthPort: number,
  apiKey: string,
): Promise<string> {
  await mkdir(directory, { recursive: true });

  const configPath = path.join(directory, "config.dist.yml");
  const yaml = [
    "env: dev",
    "",
    "app_project: tls-client",
    "app_family: tls-client",
    "app_name: api",
    "",
    "log:",
    "  handlers:",
    "    main:",
    "      formatter: console",
    "      level: info",
    "      type: iowriter",
    "      writer: stdout",
    "  timestamp_format: '15:04:05:000'",
    "",
    "sentry:",
    "  dsn: ''",
    "  release: ''",
    "  tags:",
    "    project: tls-client",
    "    component: tls-client-api",
    "",
    "api:",
    `  port: ${port}`,
    "  mode: release",
    "  health:",
    `    port: ${healthPort}`,
    "  timeout:",
    "    read: 120s",
    "    write: 120s",
    "    idle: 120s",
    "",
    `api_auth_keys: [\"${apiKey}\"]`,
    'api_cors_allowed_origin_pattern: ""',
    'api_cors_allowed_headers: ["X-API-KEY", "X-API-VIEW", "Content-Type"]',
    'api_cors_allowed_methods: ["POST", "GET", "PUT", "DELETE"]',
    "",
  ].join("\n");

  await writeFile(configPath, yaml, { encoding: "utf8", mode: 0o600 });

  if (process.platform !== "win32") {
    await chmod(configPath, 0o600);
  }

  return configPath;
}
