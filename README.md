# gonka-flow-snapshots

Rolling crash-recovery snapshots for [gonka-flow](https://github.com/loca555/gonka-flow).

The `auto-snapshot` release (prerelease) always holds the latest verified
public seed archive `wgnk.sqlite3.gz` and its `manifest.json`. The service
downloads it anonymously at boot and picks it only when it is newer than the
deploy-bundled seed. No code lives here.
