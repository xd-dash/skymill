{
  description = "Skymill HTTP provider-only OCI image (external Redis)";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      each = f: nixpkgs.lib.genAttrs systems (system: f system);
    in {
      packages = each (system:
        let
          pkgs = import nixpkgs { inherit system; };
          skymillHTTP = pkgs.buildGoModule {
            pname = "skymill-http";
            version = "0.1.0";
            src = self;
            subPackages = [ "cmd/skymill-http" ];
            # Fail closed until a Nix build establishes the dependency vendor hash.
            vendorHash = pkgs.lib.fakeHash;
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
