{
  description = "Fitbit data processing environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            python3
            uv
            postgresql
            stdenv.cc.cc.lib
          ];

          shellHook = ''
            echo "Fitbit data processing environment loaded"
            echo "Use 'uv' to manage Python packages"
          '';
        };
      });
}