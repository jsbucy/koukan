
# cribbed from
# https://github.com/bazelbuild/examples/tree/main/rules/test_rule/line_length.bzl

# some inspiration from
# https://github.com/bazel-contrib/bazel-mypy-integration

# TODO I can't believe I have to handroll this, Google should
# release theirs! This has ~2 defects:
# 1: not hermetic on pytype, runs it with the shell from whatever is
# in your path
# 2: it might be preferable to do this static analysis as part of the
# build step
# https://stackoverflow.com/questions/70962005/can-i-add-static-analysis-to-a-py-binary-or-py-library-rule
# possibly via validation actions
# https://bazel.build/extending/rules#validation-actions

def _pytype_lib_test_impl(ctx):
    srcs = ctx.files.srcs

    cmds = [_run_pytype(f.path) for f in srcs]
    script = "\n".join(["err=0"] + cmds + ["exit $err"])

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )

    runfiles = ctx.runfiles(files = srcs)
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



# first attempt left for posterity
# the idea was to write
# py_library(name='my_py_library', ...)
# pytype_test(deps=[':my_py_library'])
# but I couldn't find a way to get ahold of the direct sources, only
# PyInfo.transitive_sources which is going to scale ~quadratically and
# was already noticeably bad at the modest scale of the codebase at
# the time I initially did this

def _run_pytype(f):
    return 'pytype {f} || err=1'.format(f=f)

def _pytype_test_impl(ctx):
    srcs = []
    for dep in ctx.attr.deps:
        srcs.extend(dep[PyInfo].transitive_sources.to_list())

    cmds = [_run_pytype(f.path) for f in srcs]
    script = "\n".join(["err=0"] + cmds + ["exit $err"])

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )

    runfiles = ctx.runfiles(files = srcs)
    return [DefaultInfo(runfiles = runfiles)]

_pytype_test = rule(
    implementation = _pytype_test_impl,
    attrs = {
        "deps": attr.label_list(),
    },
    test = True,
)

def pytype_test(name, **kwargs):
    return

    if 'tags' not in kwargs:
        kwargs['tags'] = []
    kwargs['tags'].append('pytype_test')
    _pytype_test(name=name, **kwargs)
