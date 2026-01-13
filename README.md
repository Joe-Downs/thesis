# Masters Thesis

Repository for most/all files pertaining to my Masters thesis. Will certainly
include all the relevant code and the LaTeX for the thesis document itself. May
add my notes from `roam-notes`. That might be troublesome since those notes are
quite tightly integrated with each other and I could not easily separate the
relevant notes from my personal/school/etc notes.


## Thesis Document

Mainly just the LaTeX source for the final deliverable document itself. It could
also include code used in producing the report.


## Relevant Code

This includes code which is *directly* related to the actual thesis. This will
likely include code for experimental tests, data analysis, visualization, etc.


## Misc Code

Any other code that is just for proof-of-concepts or otherwise not really
related to the final deliverable.


## Cloning

Since we make use of a submodule for [shared TeX
config](https://github.com/Joe-Downs/tex-cfg), be sure to clone the project with
`git clone --recursive git@github.com:Joe-Downs/thesis.git`. If you forget this
step, then run `git submodule init` and `git submodule update` in the cloned
repo.
