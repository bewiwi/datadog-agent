"""
Android namespaced tasks
"""
from __future__ import print_function
import glob
import yaml
import os
import shutil
import sys
import platform
from distutils.dir_util import copy_tree

import invoke
from invoke import task
from invoke.exceptions import Exit

from .utils import bin_name, get_build_flags, get_version_numeric_only, load_release_versions
from .utils import REPO_PATH
from .build_tags import get_build_tags, get_default_build_tags, LINUX_ONLY_TAGS, DEBIAN_ONLY_TAGS
from .go import deps

# constants
BIN_PATH = os.path.join(".", "bin", "agent")
AGENT_TAG = "datadog/agent:master"
from .agent import DEFAULT_BUILD_TAGS

ANDROID_CORECHECKS = [
    "cpu",
    "io",
    "load",
    "memory",
    "uptime",
]
CORECHECK_CONFS_DIR = "cmd/agent/android/app/src/main/assets/conf.d"
@task
def build(ctx, rebuild=False, race=False, build_include=None, build_exclude=None,
        use_embedded_libs=False, development=True, precompile_only=False,
          skip_assets=False):
    """
    Build the android apk. If the bits to include in the build are not specified,
    the values from `invoke.yaml` will be used.

    Example invokation:
        inv android.build 
    """
    # ensure BIN_PATH exists
    if not os.path.exists(BIN_PATH):
        os.makedirs(BIN_PATH)

    # put the check confs in place
    assetconfigs(ctx)

    build_include = DEFAULT_BUILD_TAGS if build_include is None else build_include.split(",")
    build_exclude = [] if build_exclude is None else build_exclude.split(",")

    ldflags, gcflags, env = get_build_flags(ctx, use_embedded_libs=use_embedded_libs)

    if not sys.platform.startswith('linux'):
        for ex in LINUX_ONLY_TAGS:
            if ex not in build_exclude:
                build_exclude.append(ex)

    # remove all tags that are only available on debian distributions
    distname = platform.linux_distribution()[0].lower()
    if distname not in ['debian', 'ubuntu']:
        for ex in DEBIAN_ONLY_TAGS:
            if ex not in build_exclude:
                build_exclude.append(ex)

    build_tags = get_default_build_tags(puppy=True)

    build_tags.add("android")
    cmd = "gomobile bind -target android {race_opt} {build_type} -tags \"{go_build_tags}\" "

    cmd += "-o {agent_bin} -gcflags=\"{gcflags}\" -ldflags=\"{ldflags}\" {REPO_PATH}/cmd/agent/android"
    args = {
        "race_opt": "-race" if race else "",
        "build_type": "-a" if rebuild else ("-i" if precompile_only else ""),
        "go_build_tags": " ".join(build_tags),
        "agent_bin": os.path.join(BIN_PATH, bin_name("ddagent", android=True)),
        "gcflags": gcflags,
        "ldflags": ldflags,
        "REPO_PATH": REPO_PATH,
    }
    ctx.run(cmd.format(**args), env=env)

    pwd = os.getcwd()
    os.chdir("cmd/agent/android")
    if sys.platform == 'win32':
        cmd = "gradlew.bat build"
    else:
        cmd = "./gradlew build"
    ctx.run(cmd)
    os.chdir(pwd)
    shutil.copyfile("cmd/agent/android/app/build/outputs/apk/release/app-release-unsigned.apk", "bin/agent/ddagent-release-unsigned.apk")


@task
def sign_apk(ctx, development=True):
    """
    Signs the APK with the default platform signature.
    """
    cmd = "java -jar signapk.jar platform.x509.pem platform.pk8 bin/agent/ddagent-release-unsigned.apk bin/agent/ddagent-release-signed.apk"
    ctx.run(cmd)


@task
def install(ctx, rebuild=False, race=False, build_include=None, build_exclude=None,
        skip_build=False):
    """
    Installs the APK on a device.

    By default it builds the agent before executing it, unless --skip-build was
    passed. It accepts the same set of options as agent.build.
    """
    if not skip_build:
        build(ctx, rebuild, race, build_include, build_exclude)

    sign_apk(ctx)
    cmd = "adb install -r bin/agent/ddagent-release-signed.apk"
    ctx.run(cmd)


@task
def clean(ctx):
    """
    Remove temporary objects and binary artifacts
    """
    # go clean
    print("Executing go clean")
    ctx.run("go clean")

    # remove the bin/agent folder
    print("Remove agent binary folder")
    shutil.rmtree("./bin/agent")

    shutil.rmtree(CORECHECK_CONFS_DIR)

@task
def assetconfigs(ctx):
    # move the core check config
    try:
        shutil.rmtree(CORECHECK_CONFS_DIR)
    except:
        ## it's ok if the dir is not there
        pass

    files = {}
    files_list = []
    os.makedirs(CORECHECK_CONFS_DIR)
    for check in ANDROID_CORECHECKS:
        srcfile = "cmd/agent/dist/conf.d/{}.d/conf.yaml.default".format(check)
        tgtfile = "{}/{}.yaml".format(CORECHECK_CONFS_DIR, check)
        shutil.copyfile(srcfile, tgtfile)
        files_list.append("{}.yaml".format(check))
    files["files"] = files_list

    with open("{}/directory_manifest.yaml".format(CORECHECK_CONFS_DIR), 'w') as outfile:
        yaml.dump(files, outfile, default_flow_style=False)

@task
def launchservice(ctx, api_key, hostname=None, tags=None):
    if api_key is None:
        print("must supply api key")
        return
    
    if hostname is None:
        print("Setting hostname to android-tablet")
        hostname="android-tablet"
    
    if tags is None:
        print("Setting tags to owner:db,env:local,role:windows")
        tags="owner:db,env:local,role:windows"

    cmd = "adb shell am startservice --es api_key {} --es hostname {} --es tags {} org.datadog.agent/.DDService".format(api_key, hostname, tags)
    ctx.run(cmd)

@task   
def stopservice(ctx):
    cmd = "adb shell am force-stop org.datadog.agent"
    ctx.run(cmd)
