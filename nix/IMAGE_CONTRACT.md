# Component-owned Skymill image

The `#image` output packages **only** the `skymill-http` executable plus CA certificates, not Logma, Probot, Redis or Nginx. This is an artifact-consumer flake: it does not independently compile Go sources. The exact binary must first be built from the selected Skymill SHA by Huram/Smoke, and source provenance must be verified by that workflow.

Input contract (`runtime-artifacts/source.json`):

```json
{"skymill_sha":"e981447bacdd6dca6698c8ef5a7a74b91244132a"}
```

The input directory must contain executable `bin/skymill-http`. The SHA in the manifest is a provenance *claim*, not cryptographic proof of the binary's origin. Huram must verify its build input and artifact digest, then record both in immutable evidence. Do not allow an arbitrary local artifact directory to obtain deployment authority merely by supplying a plausible manifest.

For local Nix evaluation/build, supply the prepared exact-source artifact directory:

```sh
nix build path:./skymill#image --override-input runtime-artifacts path:./runtime-artifacts --no-write-lock-file
```

The package-owned runtime should be deployed as its own Quadlet with a private HTTP listener and scoped Redis principal. Its HTTP API currently uses `httpapi.New(stream, nil)` in the qualified source: this is not an authenticated public API. Never expose it directly to the Internet. Package-owned Redis ACL requirements remain the authority for permitted stream keys and commands; a shared Redis endpoint does not mean shared credentials.

This branch is **not qualified**. Gates: exact Go source compilation and digest, Nix build, image content inspection, positive/negative Redis ACL tests, crash/reclaim/ACK, HTTP listener isolation, and disposable FCOS systemd/Quadlet startup.
