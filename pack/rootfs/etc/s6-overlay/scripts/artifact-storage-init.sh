#!/command/with-contenv /bin/bash
# shellcheck shell=bash
set -o errexit -o nounset -o pipefail
exec python -m gpustack.artifacts.builtin
