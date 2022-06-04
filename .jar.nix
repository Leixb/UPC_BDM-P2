{ lib
, stdenv
, maven
, jre8
, makeWrapper
, callPackage
}:

let
  repository = callPackage ./.build-maven-repo.nix { };

  lines = lib.splitString "\n" (builtins.readFile ./.classpath.list);
  classpath = lib.concatMapStringsSep ":" (x: "${repository}/${x}") lines;
in

stdenv.mkDerivation rec {
  pname = "spark";
  version = "0.0.1-SNAPSHOT";

  src = ./.;

  nativeBuildInputs = [ maven makeWrapper ];

  buildPhase = ''
    runHook preBuild

    echo "Using repository ${repository}"
    mvn --offline -Dmaven.repo.local=${repository} package;

    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall

    install -Dm644 target/${pname}-${version}.jar $out/share/java/${pname}-${version}.jar

    makeWrapper ${jre8}/bin/java $out/bin/${pname} \
      --add-flags "-classpath $out/share/java/${pname}-${version}.jar:${classpath}" \
      --add-flags "Main"

    runHook postInstall
  '';
}
