{
  description = "Skymill HTTP provider-only OCI image (external Redis)";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      each = f: nixpkgs.lib.genAttrs systems (system: f system);
    in {

      # Component-owned release gates. Run locally through nix-alpine's Nix
      # container; neither gate publishes an image or writes release authority.
      apps = each (system:
        let
          pkgs = import nixpkgs { inherit system; };
          source = self;
          qualify = pkgs.writeShellApplication {
            name = "skymill-qualify";
            runtimeInputs = [ pkgs.go pkgs.git pkgs.coreutils ];
            text = ''
              set -euo pipefail
              export GOTOOLCHAIN=local
              cd "${source}"
              go test ./...
            '';
          };
          integration = pkgs.writeShellApplication {
            name = "skymill-qualify-integration";
            runtimeInputs = [ pkgs.go pkgs.redis pkgs.python3 pkgs.coreutils pkgs.gnugrep ];
            text = ''
              set -euo pipefail
              export GOTOOLCHAIN=local
              work="$(mktemp -d)"
              cleanup() {
                if [[ -n "${redis_pid:-}" ]]; then kill "$redis_pid" 2>/dev/null || true; wait "$redis_pid" 2>/dev/null || true; fi
                rm -rf "$work"
              }
              trap cleanup EXIT
              port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
              redis-server --bind 127.0.0.1 --port "$port" --save "" --appendonly no --dir "$work" --daemonize no > "$work/redis.log" 2>&1 &
              redis_pid=$!
              for i in $(seq 1 50); do
                if redis-cli -h 127.0.0.1 -p "$port" ping >/dev/null 2>&1; then break; fi
                kill -0 "$redis_pid" || { cat "$work/redis.log" >&2; exit 1; }
                sleep 0.1
              done
              redis-cli -h 127.0.0.1 -p "$port" ping | grep -qx PONG
              export SKYMILL_TEST_REDIS_ADDR="127.0.0.1:$port"
              export SKYMILL_TEST_REDIS_URL="redis://127.0.0.1:$port/0"
              export SKYMILL_TEST_REDIS_DESTRUCTIVE=YES
              cd "${source}"
              # Separate package processes: the root tests flush their dedicated
              # Redis DB, so do not run integration packages concurrently.
              go test -count=1 .
              go test -count=1 ./httpapi
            '';
          };
        in {
          qualify = { type = "app"; program = "${qualify}/bin/skymill-qualify"; };
          qualify-integration = { type = "app"; program = "${integration}/bin/skymill-qualify-integration"; };
        });
      packages = each (system:
        let
          pkgs = import nixpkgs { inherit system; };
          skymillHTTP = pkgs.buildGoModule {
            pname = "skymill-http";
            version = "0.1.0";
            src = self;
            subPackages = [ "cmd/skymill-http" ];
            # Fail closed until a Nix build establishes the dependency vendor hash.
            vendorHash = "sha256-9rGVZ7O1IloMHHL/f6Sn9zRHfCjCbviiwSGx2eaxEVQ=";
          };
          providerImage = pkgs.dockerTools.buildLayeredImage {
            name = "skymill-provider";
            tag = "candidate";
            contents = [ skymillHTTP pkgs.cacert ];
            config = {
              Entrypoint = [ "${skymillHTTP}/bin/skymill-http" ];
              Env = [ "REDIS_URL=unix:///run/redis/redis.sock" ];
              WorkingDir = "/";
            };
            maxLayers = 100;
          };
        in {
          default = providerImage;
          provider-image = providerImage;
          skymill-http = skymillHTTP;
        });
    };
}
