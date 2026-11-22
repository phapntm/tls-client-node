// Download/cache smoke tests. Network responses and binary contents are fixtures;
// no real release is downloaded and no runtime executable is started.
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const fs = require("node:fs/promises");
const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const { ensureBinary, ensureNativeLibrary } = require(path.join(rootDir, "dist", "binary.js"));
const originalFetch = globalThis.fetch;
const originalPlatform = Object.getOwnPropertyDescriptor(process, "platform");
const originalArch = Object.getOwnPropertyDescriptor(process, "arch");
const originalWriteFile = fs.writeFile;
const originalRename = fs.rename;
const temporaryRoot = await fs.mkdtemp(path.join(os.tmpdir(), "tls-client-download-smoke-"));
const scenariosChecked = [];
const watchdog = setTimeout(() => {
  console.error("Binary download smoke tests timed out.");
  process.exit(1);
}, 15_000);

function deferred() {
  let resolve;
  const promise = new Promise((done) => { resolve = done; });
  return { promise, resolve };
}

function fixture(platform) {
  const names = platform === "linux"
    ? { api: ["tls-client-api-linux-amd64-", ""], native: ["tls-client-linux-ubuntu-amd64-", ".so"] }
    : { api: ["tls-client-api-windows-64-", ".exe"], native: ["tls-client-windows-64-", ".dll"] };
  const metadata = [];
  const downloads = [];
  const state = { metadataError: undefined, downloadStatus: 200, bodyError: undefined };
  const filename = (kind, version = "1.18.1") => `${names[kind][0]}${version}${names[kind][1]}`;
  const content = (kind, version = "1.18.1") => Buffer.from(`fixture-${kind}-${version}`);

  globalThis.fetch = async (input, options) => {
    const url = String(input);
    if (url.startsWith("https://api.github.com/repos/")) {
      metadata.push({ url, options });
      if (state.metadataError) throw state.metadataError;
      const kind = url.includes("/tls-client-api/") ? "api" : "native";
      const version = url.endsWith("/latest") ? "1.18.1" : url.split("/tags/v")[1];
      assert.ok(version, `Unexpected metadata URL: ${url}`);
      return new Response(JSON.stringify({
        tag_name: `v${version}`,
        assets: [{
          name: filename(kind, version),
          browser_download_url: `https://assets.test/${kind}/${version}`,
        }],
      }));
    }

    const match = /^https:\/\/assets\.test\/(api|native)\/([^/]+)$/.exec(url);
    assert.ok(match, `Unexpected download URL: ${url}`);
    downloads.push(url);
    if (state.downloadStatus !== 200) {
      return new Response("unavailable", { status: state.downloadStatus });
    }
    if (state.bodyError) {
      return new Response(new ReadableStream({
        start(controller) {
          controller.enqueue(new Uint8Array([1, 2, 3]));
          controller.error(state.bodyError);
        },
      }));
    }
    return new Response(content(match[1], match[2]));
  };

  return { metadata, downloads, state, filename, content };
}

async function checkResults(results, filepath, content) {
  assert.ok(results.every((result) => (result.executablePath ?? result.libraryPath) === filepath));
  assert.ok(results.every((result) => result.version === "1.18.1"));
  assert.deepEqual(await fs.readFile(filepath), content);
  assert.deepEqual(await fs.readdir(path.dirname(filepath)), [path.basename(filepath)]);
}

function checkSharedFailure(results, codeOrError) {
  assert.ok(results.every((result) => result.status === "rejected"));
  const error = results[0].reason;
  assert.ok(results.every((result) => result.reason === error));
  if (typeof codeOrError === "string") assert.equal(error.code, codeOrError);
  else assert.equal(error, codeOrError);
}

async function scenario(platform, name, run) {
  const directory = path.join(temporaryRoot, platform, name);
  await fs.mkdir(directory, { recursive: true });
  const network = fixture(platform);
  try {
    await run(directory, network);
    scenariosChecked.push(`${platform}: ${name}`);
  } finally {
    fs.writeFile = originalWriteFile;
    fs.rename = originalRename;
  }
}

try {
  // Run the loader with both platform selections, including Ubuntu x64.
  // This simulates selection on Windows; it does not execute Linux binaries.
  for (const platform of ["linux", "win32"]) {
    Object.defineProperty(process, "platform", { ...originalPlatform, value: platform });
    Object.defineProperty(process, "arch", { ...originalArch, value: "x64" });

    for (const [kind, ensure] of [["api", ensureBinary], ["native", ensureNativeLibrary]]) {
      await scenario(platform, `${kind}-cold-and-warm`, async (directory, network) => {
        const options = { downloadDir: directory };
        const results = await Promise.all(Array.from({ length: 10 }, () => ensure(options)));
        const filepath = path.join(directory, network.filename(kind));
        await checkResults(results, filepath, network.content(kind));
        assert.equal(network.metadata.length, 1);
        assert.equal(network.downloads.length, 1);

        await Promise.all(Array.from({ length: 10 }, () => ensure(options)));
        assert.equal(network.metadata.length, 1);
        assert.equal(network.downloads.length, 1);

        // A completed promise must not mask a later removal of the cached file.
        await fs.unlink(filepath);
        await ensure(options);
        assert.equal(network.metadata.length, 2);
        assert.equal(network.downloads.length, 2);
      });
    }

    await scenario(platform, "equivalent-paths-and-versions", async (directory, network) => {
      await Promise.all(Array.from({ length: 10 }, (_, i) => ensureBinary({
        downloadDir: i % 2 ? `${directory}${path.sep}.` : directory,
        version: i % 2 ? "v1.18.1" : "1.18.1",
      })));
      assert.equal(network.metadata.length, 1);
      assert.equal(network.downloads.length, 1);
      assert.ok(network.metadata[0].url.endsWith("/tags/v1.18.1"));
    });

    await scenario(platform, "different-versions", async (directory, network) => {
      const results = await Promise.all(["1.18.1", "1.16.0"].map((version) =>
        ensureBinary({ downloadDir: directory, version }),
      ));
      assert.equal(network.metadata.length, 2);
      assert.equal(network.downloads.length, 2);
      assert.deepEqual(results.map((result) => result.version), ["1.18.1", "1.16.0"]);
      assert.notEqual(results[0].executablePath, results[1].executablePath);
    });

    await scenario(platform, "different-directories", async (directory, network) => {
      const results = await Promise.all(["a", "b"].map((name) =>
        ensureBinary({ downloadDir: path.join(directory, name) }),
      ));
      assert.equal(network.downloads.length, 2);
      assert.notEqual(results[0].executablePath, results[1].executablePath);
      for (const result of results) assert.deepEqual(await fs.readFile(result.executablePath), network.content("api"));
    });

    await scenario(platform, "api-and-native-independent", async (directory, network) => {
      const [api, native] = await Promise.all([
        ensureBinary({ downloadDir: directory }),
        ensureNativeLibrary({ downloadDir: directory }),
      ]);
      assert.equal(network.metadata.length, 2);
      assert.equal(network.downloads.length, 2);
      assert.notEqual(api.executablePath, native.libraryPath);
      assert.deepEqual(await fs.readFile(api.executablePath), network.content("api"));
      assert.deepEqual(await fs.readFile(native.libraryPath), network.content("native"));
    });

    await scenario(platform, "metadata-failure-and-retry", async (directory, network) => {
      const error = new Error("metadata network failure");
      network.state.metadataError = error;
      const failed = await Promise.allSettled(Array.from({ length: 10 }, () =>
        ensureBinary({ downloadDir: directory }),
      ));
      checkSharedFailure(failed, error);
      assert.equal(network.metadata.length, 1);
      assert.equal(network.downloads.length, 0);
      network.state.metadataError = undefined;
      await Promise.all(Array.from({ length: 10 }, () => ensureBinary({ downloadDir: directory })));
      assert.equal(network.metadata.length, 2);
      assert.equal(network.downloads.length, 1);
    });

    for (const failure of ["http", "body", "publish"]) {
      await scenario(platform, `${failure}-failure-and-retry`, async (directory, network) => {
        const error = new Error(`${failure} failure`);
        if (failure === "http") network.state.downloadStatus = 503;
        if (failure === "body") network.state.bodyError = error;
        if (failure === "publish") fs.rename = async () => { throw error; };
        const failed = await Promise.allSettled(Array.from({ length: 10 }, () =>
          ensureBinary({ downloadDir: directory }),
        ));
        checkSharedFailure(failed, failure === "http" ? "ERR_BINARY_DOWNLOAD" : error);
        assert.equal(network.metadata.length, 1);
        assert.equal(network.downloads.length, 1);
        assert.deepEqual(await fs.readdir(directory), []);

        network.state.downloadStatus = 200;
        network.state.bodyError = undefined;
        fs.rename = originalRename;
        const retried = await Promise.all(Array.from({ length: 10 }, () =>
          ensureBinary({ downloadDir: directory }),
        ));
        await checkResults(retried, path.join(directory, network.filename("api")), network.content("api"));
        assert.equal(network.metadata.length, 2);
        assert.equal(network.downloads.length, 2);
      });
    }

    await scenario(platform, "atomic-publish-and-distinct-selectors", async (directory, network) => {
      const partiallyWritten = deferred();
      const releaseWrite = deferred();
      fs.writeFile = async (file, data, options) => {
        if (path.dirname(String(file)) === directory && String(file).endsWith(".tmp")) {
          await originalWriteFile(file, data.subarray(0, 3), options);
          partiallyWritten.resolve();
          await releaseWrite.promise;
          return originalWriteFile(file, data);
        }
        return originalWriteFile(file, data, options);
      };

      const first = ensureBinary({ downloadDir: directory, version: "1.18.1" });
      let second;
      try {
        await partiallyWritten.promise;
        const files = await fs.readdir(directory);
        assert.equal(files.length, 1);
        assert.ok(files[0].startsWith("."));
        assert.ok(files[0].endsWith(".tmp"));
        let secondFinished = false;
        second = ensureBinary({ downloadDir: directory }).then((result) => {
          secondFinished = true;
          return result;
        });
        await new Promise((resolve) => setImmediate(resolve));
        assert.equal(secondFinished, false);
      } finally {
        releaseWrite.resolve();
        await Promise.allSettled([first, second]);
      }
      const results = await Promise.all([first, second]);
      await checkResults(results, path.join(directory, network.filename("api")), network.content("api"));
      assert.equal(network.downloads.length, 1);
    });

    await scenario(platform, "explicit-latest-still-refreshes", async (directory, network) => {
      const options = { downloadDir: directory, version: "latest" };
      await Promise.all(Array.from({ length: 10 }, () => ensureBinary(options)));
      assert.equal(network.metadata.length, 1);
      assert.equal(network.downloads.length, 1);
      await ensureBinary(options);
      assert.equal(network.metadata.length, 2);
      assert.equal(network.downloads.length, 2);
    });

    await scenario(platform, "explicit-paths-bypass-downloads", async (directory, network) => {
      const binaryPath = path.join(directory, "custom-api");
      const nativeLibraryPath = path.join(directory, "custom-native");
      assert.equal((await ensureBinary({ binaryPath })).executablePath, binaryPath);
      assert.equal((await ensureNativeLibrary({ nativeLibraryPath })).libraryPath, nativeLibraryPath);
      assert.equal(network.metadata.length, 0);
      assert.equal(network.downloads.length, 0);
    });
  }
} finally {
  clearTimeout(watchdog);
  globalThis.fetch = originalFetch;
  Object.defineProperty(process, "platform", originalPlatform);
  Object.defineProperty(process, "arch", originalArch);
  fs.writeFile = originalWriteFile;
  fs.rename = originalRename;
  // Only remove the absolute, uniquely created directory owned by this test.
  assert.equal(path.dirname(path.resolve(temporaryRoot)), path.resolve(os.tmpdir()));
  assert.ok(path.basename(temporaryRoot).startsWith("tls-client-download-smoke-"));
  await fs.rm(temporaryRoot, { recursive: true, force: true });
}

console.log(JSON.stringify({ ok: true, scenariosChecked }));
