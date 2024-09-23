# cribbed from
# https://github.com/bazelbuild/examples/tree/main/rules/test_rule/line_length.bzl

# some inspiration from
# https://github.com/bazel-contrib/bazel-mypy-integration

# TODO this effectively leaves the pytype output directory in the
# bazel sandbox directory so it starts from scratch every time. I
# almost got this working running pytype-single directly and making
# the .pyi an output but got stuck on non-hermetic dependencies
# (site-packages, etc), I think it's a tiny patch to pytype to support
# this and plan to take it up with them

def _run_pytype(f):
    return 'pytype -v2 {f} || err=1'.format(f=f)

def _pytype_lib_test_impl(ctx):
    srcs = ctx.files.srcs
    runfiles = ctx.runfiles(files = srcs)
    transitive_runfiles = []
    for runfiles_attr in (
        ctx.attr.srcs,
        ctx.attr.deps):
        for target in runfiles_attr:
            transitive_runfiles.append(target[DefaultInfo].default_runfiles)
    runfiles = runfiles.merge_all(transitive_runfiles)

    cmds = [ _run_pytype(f.path) for f in srcs ]
    script = "\n".join(["err=0"] + cmds + ["exit $err"])

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )

    return [DefaultInfo(runfiles = runfiles)]

_pytype_lib_test = rule(
    implementation = _pytype_lib_test_impl,
    attrs = {
        "deps": attr.label_list(),
        "srcs": attr.label_list(allow_files = [".py"]),
    },
    test = True,
)

def pytype_lib_test(name, **kwargs):
    if 'tags' not in kwargs:
        kwargs['tags'] = []
    kwargs['tags'].append('pytype_test')
    _pytype_lib_test(name=name, **kwargs)

def pytype_library(name, srcs, deps=None, data=None, **kwargs):
  native.py_library(name=name,
                    srcs=srcs,
                    deps=deps,
                    data=data,
                    **kwargs)

  # don't propagate data
  pytype_lib_test(name=(name + '_pytype_test'),
                  srcs=srcs,
                  deps=deps,
                  **kwargs)


