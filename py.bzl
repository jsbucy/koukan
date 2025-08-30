load("@pip_types//:types.bzl", "types")
load("@rules_mypy//mypy:mypy.bzl", "mypy")

mypy_aspect = mypy(types = types,
                   mypy_ini = "@@//:mypy.ini",
                   suppression_tags = ["no-mypy", "no-checks"])
