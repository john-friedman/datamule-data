# datamule data generators

This `scripts` branch contains the code and configuration used to generate the data published on the `master` branch.

Clone only this branch to work on the generators without downloading the data history:

```sh
git clone --branch scripts --single-branch https://github.com/john-friedman/datamule-data.git
```

The scheduled workflows live on `master`. Each workflow checks out this branch in `.scripts/`, runs its generator from the `master` working directory, then commits the updated files to `master`. The daily generator reads `data.json` from this branch and reads or writes `data/` and `updates.json` in the working directory.
