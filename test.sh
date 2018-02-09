#!/bin/zsh

VALGRIND_OPT=( "--tool=memcheck" "--track-origins=yes" "--read-var-info=yes" )

VG_MEMCHECK_OPT=( "--keep-stacktraces=alloc-and-free")
VG_LEAKCHECK_OPT=("--leak-check=full" "--show-leak-kinds=all" "--leak-check-heuristics=all")

#expensive definedness checks (newish option)
VG_MEMCHECK_OPT+=( "--expensive-definedness-checks=yes")

#long stack traces
VG_MEMCHECK_OPT+=("--num-callers=20")

#generate suppresions
#VG_MEMCHECK_OPT+=("--gen-suppressions=all")

#track files
#VG_MEMCHECK_OPT+=("--track-fds=yes")



for opt in $*; do
  case $opt in
    valgrind|memcheck)
      valgrind=1
      VALGRIND_OPT+=($VG_MEMCHECK_OPT);;
    leak|leakcheck)
      valgrind=1
      VALGRIND_OPT+=($VG_LEAKCHECK_OPT);;
    luajit)
      luajit=1;;
    5.1)
      lua51=1;;
    5.2)
      lua52=1;;
    debug-memcheck)
      valgrind=1
      VALGRIND_OPT+=($VG_MEMCHECK_OPT)
      VALGRIND_OPT+=( "--vgdb=yes" "--vgdb-error=1" )
      #ATTACH_DDD=1
      ;;
    massif)
      VALGRIND_OPT=( "--tool=massif" "--heap=yes" "--stacks=yes" "--massif-out-file=massif-nginx-%p.out")
      valgrind=1
      ;;
    sanitize-undefined)
      FSANITIZE_UNDEFINED=1
      ;;
    callgrind|profile)
      VALGRIND_OPT=( "--tool=callgrind" "--collect-jumps=yes"  "--collect-systime=yes" "--branch-sim=yes" "--cache-sim=yes" "--simulate-hwpref=yes" "--simulate-wb=yes" "--callgrind-out-file=callgrind-prailude-%p.out")
      valgrind=1;;
    helgrind)
    VALGRIND_OPT=( "--tool=helgrind" "--free-is-write=yes")
    valgrind=1
    ;;
    cachegrind)
      VALGRIND_OPT=( "--tool=cachegrind" )
      valgrind=1;;
    debug)
      debugger=1
      ;;
  esac
done


if [[ $debugger == 1 ]]; then
  kdbg ./lua -a ./test.lua
elif [[ $valgrind == 1 ]]; then
  valgrind $VALGRIND_OPT ./lua ./test.lua
elif [[ $luajit == 1 ]]; then
  luajit ./test.lua
elif [[ $lua51 == 1 ]]; then
  lua5.1 ./test.lua
elif [[ $lua52 == 1 ]]; then
  lua5.2 ./test.lua
else
  ./lua ./test.lua
fi
