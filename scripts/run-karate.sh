#!/bin/bash
# Karate_kafka: check the prerequisites, run the Karate tests, report the result.
#
#   ./scripts/run-karate.sh [options]        (or: bash scripts/run-karate.sh [options])
#
# Checks Java (JDK) 21, Maven 3.9+ and Git, and that Maven Central and packages.confluent.io can be reached.
# With --install-missing it offers to install what is missing (asking first). Then it runs the Karate tests, says
# plainly whether they passed, and opens the HTML report.
#
# Options:
#   --feature NAME     run one feature only, e.g. kafka-consumer, orders-e2e, or your own (my-first)
#   --everything       run every test in the repository (Karate, Cucumber, JUnit) instead of only Karate
#   --install-missing  offer to install missing tools (Java and Git with Homebrew / Apple, Maven as a portable copy)
#   --yes              with --install-missing: do not ask, answer yes
#   --check-only       only check the prerequisites, do not run the tests
#   --no-report        do not open the HTML report afterwards
#   --show-log         show the full Maven output (very long) instead of the short summary
#   -h, --help         show this help
#
# Exit code: 0 = tests passed, 1 = tests failed, 2 = a prerequisite is missing or the options are wrong.
#
# EXPERIMENTAL on macOS: the logic was tested on Linux; the macOS-only parts (Homebrew, /usr/libexec/java_home,
# Apple Command Line Tools, "open") were not. Written for the bash 3.2 that ships with macOS.

set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MAVEN_VERSION="3.9.11"
TOOLS_DIR="${KARATE_KAFKA_TOOLS:-$HOME/.karate_kafka/tools}"

FEATURE=""
EVERYTHING=0
INSTALL=0
YES=0
CHECK_ONLY=0
NO_REPORT=0
SHOW_LOG=0

usage() {
    sed -n '2,24p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
    case "$1" in
        --feature)
            if [ $# -lt 2 ]; then echo "--feature needs a name, e.g. --feature kafka-consumer"; exit 2; fi
            FEATURE="$2"; shift 2 ;;
        --everything)      EVERYTHING=1; shift ;;
        --install-missing) INSTALL=1; shift ;;
        --yes)             YES=1; shift ;;
        --check-only)      CHECK_ONLY=1; shift ;;
        --no-report)       NO_REPORT=1; shift ;;
        --show-log)        SHOW_LOG=1; shift ;;
        -h|--help)         usage; exit 0 ;;
        *)                 echo "Unknown option: $1"; echo; usage; exit 2 ;;
    esac
done

if [ -n "$FEATURE" ]; then
    case "$FEATURE" in
        *[!A-Za-z0-9_-]*) echo "--feature may only contain letters, digits, - and _ (got: $FEATURE)"; exit 2 ;;
    esac
fi

# ---------- output helpers ----------
if [ -t 1 ]; then
    C_RED=$'\033[31m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_CYAN=$'\033[36m'; C_OFF=$'\033[0m'
else
    C_RED=""; C_GREEN=""; C_YELLOW=""; C_CYAN=""; C_OFF=""
fi
step() { printf '\n%s== %s%s\n' "$C_CYAN" "$1" "$C_OFF"; }
ok()   { printf '  %s[ OK ]%s %s\n' "$C_GREEN" "$C_OFF" "$1"; }
bad()  { printf '  %s[FAIL]%s %s\n' "$C_RED" "$C_OFF" "$1"; }
note() { printf '  %s[ !! ]%s %s\n' "$C_YELLOW" "$C_OFF" "$1"; }

confirm() {
    if [ "$YES" = 1 ]; then return 0; fi
    if [ ! -t 0 ]; then return 1; fi
    local answer
    read -r -p "  $1 (y/N) " answer
    case "$answer" in [Yy]*) return 0 ;; *) return 1 ;; esac
}

# ---------- finding and checking the tools ----------
find_brew() {
    if command -v brew >/dev/null 2>&1; then command -v brew; return 0; fi
    local candidate
    for candidate in /opt/homebrew/bin/brew /usr/local/bin/brew; do
        if [ -x "$candidate" ]; then echo "$candidate"; return 0; fi
    done
    return 1
}

# Homebrew's openjdk@21 is "keg-only": installed but not registered with macOS, so we look for it ourselves.
brew_jdk_home() {
    local brew prefix
    brew="$(find_brew)" || return 0
    prefix="$("$brew" --prefix openjdk@21 2>/dev/null)" || return 0
    if [ -x "$prefix/libexec/openjdk.jdk/Contents/Home/bin/java" ]; then
        echo "$prefix/libexec/openjdk.jdk/Contents/Home"
    fi
    return 0
}

add_portable_maven() {
    local dir best=""
    for dir in "$TOOLS_DIR"/apache-maven-*; do
        if [ -x "$dir/bin/mvn" ]; then best="$dir"; fi
    done
    if [ -n "$best" ]; then
        case ":$PATH:" in *":$best/bin:"*) ;; *) PATH="$best/bin:$PATH"; export PATH ;; esac
    fi
}

JAVA_TEXT=""
detect_java() {
    local home="" out major minor re
    # 1. a JDK 21 registered with macOS
    if [ -x /usr/libexec/java_home ]; then
        home="$(/usr/libexec/java_home -v 21 2>/dev/null)" || home=""
    fi
    # 2. Homebrew's openjdk@21
    if [ -z "$home" ]; then home="$(brew_jdk_home)"; fi
    if [ -n "$home" ] && [ -x "$home/bin/java" ]; then
        JAVA_HOME="$home"; export JAVA_HOME
        case ":$PATH:" in *":$JAVA_HOME/bin:"*) ;; *) PATH="$JAVA_HOME/bin:$PATH"; export PATH ;; esac
    fi
    # 3. whatever "java" now is on the PATH
    if ! command -v java >/dev/null 2>&1; then JAVA_TEXT="Java was not found"; return 1; fi
    out="$(java -version 2>&1)"
    re='version "([0-9]+)(\.([0-9]+))?'
    if [[ $out =~ $re ]]; then
        major="${BASH_REMATCH[1]}"; minor="${BASH_REMATCH[3]:-}"
        if [ "$major" = 1 ] && [ -n "$minor" ]; then major="$minor"; fi   # old style: 1.8 means Java 8
        if [ "$major" -lt 21 ]; then JAVA_TEXT="Java $major found, but Java 21 or newer is needed"; return 1; fi
        if [ "$major" -gt 21 ]; then JAVA_TEXT="Java $major found (21 is the tested version)"; return 0; fi
        JAVA_TEXT="Java $major"; return 0
    fi
    JAVA_TEXT="Java was not found (no Java runtime is installed)"
    return 1
}

MAVEN_TEXT=""
detect_maven() {
    local out re major minor
    add_portable_maven
    if ! command -v mvn >/dev/null 2>&1; then MAVEN_TEXT="Maven was not found"; return 1; fi
    out="$(mvn -version 2>&1)"
    re='Apache Maven ([0-9]+)\.([0-9]+)\.([0-9]+)'
    if [[ $out =~ $re ]]; then
        major="${BASH_REMATCH[1]}"; minor="${BASH_REMATCH[2]}"
        MAVEN_TEXT="Maven ${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"
        if [ "$major" -gt 3 ] || { [ "$major" -eq 3 ] && [ "$minor" -ge 9 ]; }; then return 0; fi
        MAVEN_TEXT="$MAVEN_TEXT found, but Maven 3.9 or newer is needed"
        return 1
    fi
    MAVEN_TEXT="Maven did not start properly: $(printf '%s' "$out" | head -n 1)"
    return 1
}

GIT_TEXT=""
detect_git() {
    # On a Mac without Apple's Command Line Tools, /usr/bin/git is a stub that pops up an installer window.
    # Do not trigger that by accident just to read a version number.
    if [ "$(uname)" = "Darwin" ] && [ "$(command -v git 2>/dev/null)" = "/usr/bin/git" ] && ! xcode-select -p >/dev/null 2>&1; then
        GIT_TEXT="Git is not installed yet (Apple's Command Line Tools are missing)"
        return 1
    fi
    if ! command -v git >/dev/null 2>&1; then GIT_TEXT="Git was not found"; return 1; fi
    GIT_TEXT="$(git --version 2>&1 | head -n 1)"
    return 0
}

http_ok() {
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 "$1" 2>/dev/null)" || return 1
    [ "$code" = "200" ]
}

# ---------- installers (only with --install-missing; each one asks first) ----------
sha512_of() {
    if command -v shasum >/dev/null 2>&1; then shasum -a 512 "$1" | awk '{print $1}'
    else sha512sum "$1" | awk '{print $1}'; fi
}

install_maven() {
    confirm "Download Apache Maven $MAVEN_VERSION (about 9 MB) to $TOOLS_DIR ? No admin rights needed." || return 1
    local url tgz expected actual
    url="https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"
    tgz="$TOOLS_DIR/apache-maven-$MAVEN_VERSION-bin.tar.gz"
    mkdir -p "$TOOLS_DIR" || { note "Cannot create $TOOLS_DIR"; return 1; }
    if ! curl -fsSL --max-time 300 -o "$tgz" "$url"; then
        rm -f "$tgz"; note "Could not download Maven from $url"; return 1
    fi
    expected="$(curl -fsSL --max-time 30 "$url.sha512" 2>/dev/null | awk '{print $1}')"
    actual="$(sha512_of "$tgz")"
    if [ -z "$expected" ] || [ "$expected" != "$actual" ]; then
        rm -f "$tgz"; note "The downloaded Maven file does not match its published checksum, so I removed it."; return 1
    fi
    if ! tar -xzf "$tgz" -C "$TOOLS_DIR"; then rm -f "$tgz"; note "Could not unpack Maven"; return 1; fi
    rm -f "$tgz"
    return 0
}

install_java() {
    local brew
    if ! brew="$(find_brew)"; then
        note "Homebrew is not installed. Install it from https://brew.sh (one command, shown on that page) and run this again, or install a JDK 21 yourself from https://adoptium.net"
        return 1
    fi
    confirm "Install Java 21 with Homebrew (brew install openjdk@21)?" || return 1
    if ! "$brew" install openjdk@21; then
        note "Homebrew could not install openjdk@21. Install a JDK 21 yourself from https://adoptium.net"
        return 1
    fi
    return 0
}

install_git() {
    if [ "$(uname)" = "Darwin" ] && ! xcode-select -p >/dev/null 2>&1; then
        confirm "Install Apple's Command Line Tools (this includes Git; a window will open)?" || return 1
        xcode-select --install >/dev/null 2>&1
        note "Finish the installer window that opened, then run this script again."
        return 1
    fi
    local brew
    if brew="$(find_brew)"; then
        confirm "Install Git with Homebrew (brew install git)?" || return 1
        "$brew" install git && return 0
    fi
    note "Install Git yourself: https://git-scm.com/download/mac"
    return 1
}

# ======================= main =======================
echo "Karate_kafka: Kafka producer/consumer integration tests (Karate proof of concept)"

step "1. Checking prerequisites"

if ! detect_java && [ "$INSTALL" = 1 ]; then
    if install_java; then detect_java; fi
fi
if detect_java; then ok "$JAVA_TEXT"; JAVA_OK=1; else bad "$JAVA_TEXT"; JAVA_OK=0; fi

if ! detect_maven && [ "$INSTALL" = 1 ]; then
    if install_maven; then detect_maven; fi
fi
if detect_maven; then ok "$MAVEN_TEXT"; MAVEN_OK=1; else bad "$MAVEN_TEXT"; MAVEN_OK=0; fi

if ! detect_git && [ "$INSTALL" = 1 ]; then
    if install_git; then detect_git; fi
fi
if detect_git; then ok "$GIT_TEXT"; else note "$GIT_TEXT. Only needed to clone the code or undo edits with git; the tests do not need it."; fi

CENTRAL_OK=0; CONFLUENT_OK=0
if http_ok "https://repo.maven.apache.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.pom"; then
    CENTRAL_OK=1; ok "Maven Central is reachable"
else note "Maven Central (repo.maven.apache.org) is not reachable from here"; fi
if http_ok "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.6.1/kafka-avro-serializer-7.6.1.pom"; then
    CONFLUENT_OK=1; ok "packages.confluent.io is reachable"
else note "packages.confluent.io is not reachable from here"; fi
if [ "$CENTRAL_OK" = 0 ] || [ "$CONFLUENT_OK" = 0 ]; then
    note "Dependencies are downloaded on the first run. If this is not your first run, Maven may already have them; otherwise ask your network team to allow both sites (or set a proxy in ~/.m2/settings.xml)."
fi

if [ "$JAVA_OK" = 0 ] || [ "$MAVEN_OK" = 0 ]; then
    echo
    bad "A required tool is missing, so the tests cannot run."
    if [ "$INSTALL" = 0 ]; then echo "  Run again with --install-missing to be offered an install, or install the tools listed above yourself."; fi
    exit 2
fi
if [ "$CHECK_ONLY" = 1 ]; then
    echo
    ok "All required tools are present."
    exit 0
fi

# ---------- run the tests ----------
if [ "$EVERYTHING" = 1 ] && [ -n "$FEATURE" ]; then bad "Use either --everything or --feature, not both."; exit 2; fi
if [ ! -f "$REPO_ROOT/pom.xml" ]; then
    bad "pom.xml not found in $REPO_ROOT. Run this script from inside the Karate_kafka folder (scripts/run-karate.sh)."
    exit 2
fi

step "2. Running the tests (the first run downloads dependencies and can take several minutes)"
MVN_ARGS=(-B test)
if [ "$EVERYTHING" = 0 ]; then MVN_ARGS+=("-Dtest=KarateTest"); fi
if [ -n "$FEATURE" ]; then MVN_ARGS+=("-Dkarate.options=classpath:karate/$FEATURE.feature"); fi
echo "  mvn ${MVN_ARGS[*]}"

LOG="$REPO_ROOT/target/karate-run.log"
mkdir -p "$REPO_ROOT/target"
KEEP='^(scenarios:|features:|elapsed:)|Tests run:|BUILD (SUCCESS|FAILURE)|match failed|status code was'
( cd "$REPO_ROOT" && mvn "${MVN_ARGS[@]}" 2>&1 ) | tee "$LOG" | {
    count=0
    dots=0
    while IFS= read -r line; do
        count=$((count + 1))
        if [ "$SHOW_LOG" = 1 ] || [[ $line =~ $KEEP ]]; then
            if [ "$dots" = 1 ]; then printf '\n'; dots=0; fi
            printf '%s\n' "$line"
        elif [ $((count % 100)) -eq 0 ]; then
            printf '.'; dots=1
        fi
    done
}
MVN_EXIT="${PIPESTATUS[0]}"
echo

# ---------- read the result ----------
step "3. Result"
if [ "$EVERYTHING" = 1 ]; then
    TOTAL="$(grep -E 'Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+, Skipped: [0-9]+ *$' "$LOG" | tail -n 1)"
    if [ -n "$TOTAL" ]; then printf '  Tests run:%s  (tests that need Docker skip themselves)\n' "${TOTAL#*Tests run:}"; fi
else
    TOTAL="$(grep -E 'scenarios: +[0-9]+ \| passed: +[0-9]+ \| failed: +[0-9]+ *$' "$LOG" | tail -n 1)"
    if [ -n "$TOTAL" ]; then echo "  Karate $(printf '%s' "$TOTAL" | tr -s ' ')"; fi
fi

if [ "$MVN_EXIT" = 0 ]; then
    ok "PASS: all selected tests passed."
else
    bad "FAIL: some tests failed, or the build could not run."
    grep -E -m 2 -A 3 'match failed|status code was|COMPILATION ERROR|Could not resolve|Could not transfer' "$LOG" | sed 's/^/    /'
    echo "  Full output: $LOG"
fi

REPORT="$REPO_ROOT/target/karate-reports/karate-summary.html"
if [ -f "$REPORT" ]; then
    echo "  Report: $REPORT"
    if [ "$NO_REPORT" = 0 ] && command -v open >/dev/null 2>&1; then open "$REPORT"; fi
fi
if [ "$MVN_EXIT" = 0 ]; then exit 0; else exit 1; fi
