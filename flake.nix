{
  description = "Skymill component-owned runtime image from exact Huram-qualified Go artifact";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/b25309931cfda5f0b8805f462a29897eeae50168";
    runtime-artifacts = { url = "path:./nix/runtime-artifacts"; flake = false; };
  };

  outputs = { nixpkgs, runtime-artifacts, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      manifest = builtins.fromJSON (builtins.readFile "${runtime-artifacts}/source.json");
      sha = manifest.skymill_sha;
      validSha = builtins.match "[0-9a-f]{40}" sha != null;
      runtime = pkgs.runCommand "skymill-runtime" {} ''
        test -x ${runtime-artifacts}/bin/skymill-http
        mkdir -p "$out/bin"
        cp ${runtime-artifacts}/bin/skymill-http "$out/bin/skymill-http"
      '';
      image = pkgs.dockerTools.buildLayeredImage {
        name = "fatline-skymill";
        tag = sha;
        contents = [ runtime pkgs.cacert ];
        config = {
          Entrypoint = [ "${runtime}/bin/skymill-http" ];
          Env = [ "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt" ];
        };
      };
    in
      assert validSha;
      {
        packages.${system} = { inherit runtime image; default = image; };
      };
}
