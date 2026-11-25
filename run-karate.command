#!/bin/bash
# Double-click this file in Finder to check the prerequisites and run the Karate tests.
#
# First time only: macOS may refuse to open a downloaded file. Right-click it, choose Open, then Open again.
# If you get "permission denied", run once in Terminal:  chmod +x run-karate.command
# Extra options are passed on, for example:  ./run-karate.command --feature kafka-consumer
# --install-missing only offers to install a missing tool, and asks before each install.

cd "$(dirname "$0")" || exit 1
bash scripts/run-karate.sh --install-missing "$@"
status=$?
if [ $# -eq 0 ]; then
    echo
    read -r -p "Press Enter to close this window..." _
fi
exit "$status"
