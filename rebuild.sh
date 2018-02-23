#!/bin/zsh
MY_PATH="`dirname \"$0\"`"
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"
pkg_path=$MY_PATH/nginx-pkg
_src_dir=${MY_PATH}/../src
  

_clang="clang -Qunused-arguments -fcolor-diagnostics"

#clang_memcheck="-fsanitize=address,undefined -fno-omit-frame-pointer"
clang_sanitize_memory="-use-gold-plugins -fsanitize=memory -fsanitize-memory-track-origins -fno-omit-frame-pointer -fsanitize-blacklist=bl.txt"
clang_sanitize_addres="-fsanitize=address,undefined -fno-omit-frame-pointer"
export  PATH="/usr/lib/ccache/bin/:$PATH"

optimize_level=0;

export CONFIGURE_WITH_DEBUG=0

export CC="gcc"

luarocks="luarocks"

for opt in $*; do
  case $opt in
    5.1|luajit)
      luarocks="luarocks-5.1";;
    5.2)
      luarocks="luarocks-5.2";;
    clang)
      export CC=$_clang;;
    clang-sanitize|sanitize|sanitize-memory)
      export CC="CMAKE_LD=llvm-link $_clang -Xclang -cc1 $clang_sanitize_memory "
      export CLINKER=$clang
      ;;
    gcc-sanitize-undefined)
      export SANITIZE_UNDEFINED=1
      ;;
    sanitize-address)
      export CC="$_clang $clang_sanitize_addres";;
    gcc6)
      export CC=gcc-6;;
    gcc5)
      export CC=gcc-5;;
    gcc4|gcc47|gcc4.7)
      export CC=gcc-4.7;;
    O0)
      optimize_level=0;;
    O1)
      optimize_level=1;;
    O2)
      optimize_level=2;;
    O3)
      optimize_level=3;;
    Og)
      optimize_level=g;;
    clang-analyzer|analyzer|scan|analyze)
      #export CC="clang"
      #export CLINKER=$clang
      #export CFLAGS="-ferror-limit=5 $CFLAGS -Wconditional-uninitialized"
      export CLANG_ANALYZER=$MY_PATH/clang-analyzer
      mkdir $CLANG_ANALYZER 2>/dev/null
      ;;
  esac
done

export CFLAGS="$CFLAGS -Wall -O$optimize_level -ggdb -fno-omit-frame-pointer -fPIC"

pushd src
luacheck ./ || exit 1
popd
if [[ -z $CLANG_ANALYZER ]]; then
  sudo $luarocks CC="$CC" CFLAGS="$CFLAGS" make
else
  export CC="clang"
  sudo scan-build -o "$CLANG_ANALYZER" $luarocks CC="/usr/lib/clang/ccc-analyzer" CFLAGS="$CFLAGS" LD="/usr/lib/clang/ccc-analyzer" make
fi
