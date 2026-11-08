# An example deploy repository for gitops

Copy this directory into a git repository of its own and point a Repo at
it (docs/gitops.md):

```console
$ cp -r configs/gitops/deploy ~/deploy && cd ~/deploy
$ git init -b main && git add -A && git commit -m "First deploy" && git push ...
$ legion gitops add shop --url https://github.com/<you>/deploy --path jobs --wait
```

`jobs/` holds one jobspec per file; `lib/` holds Nickel they import, which
gitops never deploys on its own.
