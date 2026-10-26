{
  description = "Skymill scoped HTTP provider — reproducible OCI image";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      eachSystem = f: nixpkgs.lib.genAttrs systems (system: f (import nixpkgs { inherit system; }));
    in {
      packages = eachSystem (pkgs:
        let
          provider = pkgs.buildGoModule {
            pname = "skymill-http";
            version = "0.1.0";
            src = ./.;
            subPackages = [ "cmd/skymill-http" ];
            go = pkgs.go_1_26;
            vendorHash = pkgs.lib.fakeHash;
            proxyVendor = true;
            env.CGO_ENABLED = "0";
          };
        in {
          provider = provider;
          provider-image = pkgs.dockerTools.buildLayeredImage {
            name = "localhost/skymill-provider";
            tag = "scoped-qualification";
            contents = [ provider pkgs.cacert ];
            config = {
              Entrypoint = [ "${provider}/bin/skymill-http" ];
              Env = [ "SKYMILL_LISTEN_ADDR=0.0.0.0:8081" ];
              ExposedPorts = { "8081/tcp" = { }; };
              User = "65534:65534";
            };
          };
          default = self.packages.${pkgs.system}.provider-image;
        });
    };
}
