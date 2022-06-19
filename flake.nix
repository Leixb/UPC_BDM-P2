{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    flake-utils = {
      url = "github:numtide/flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils }:

  flake-utils.lib.eachDefaultSystem (system:
  let
    pkgs = import nixpkgs { inherit system; };

    jar = pkgs.callPackage ./.jar.nix { };
    jdtls = pkgs.callPackage ./.jdtls.nix { };

    KAFKA_SERVER = "venomoth.fib.upc.edu:9092";
    KAFKA_TOPIC = "bdm_p2";
    DVC_NO_ANALYTICS = 1;

    commonPackages = with pkgs; [
      apacheKafka
      mongodb
      mongodb-tools
      dvc-with-remotes
      pkgs.python3.pkgs.pydrive2
      graphviz
    ];

  in rec {
    devShells = {
      default = with pkgs; mkShellNoCC {
        name = "java-kafka-mongo";
        buildInputs = commonPackages ++ [ jdk jdtls ];
        inherit KAFKA_SERVER KAFKA_TOPIC DVC_NO_ANALYTICS;
      };

      wsl = with pkgs; mkShellNoCC {
        name = "mongo-kafka";
        buildInputs = commonPackages;
        inherit KAFKA_SERVER KAFKA_TOPIC DVC_NO_ANALYTICS;
      };
    };

    packages = {
      inherit jar;
      default = self.packages.${system}.jar;
    };
  });
}
