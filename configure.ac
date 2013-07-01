#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([sqlbench], [0.1.0], [dianchengwang@gmail.com])
AM_INIT_AUTOMAKE([silent-rules])
AC_CONFIG_SRCDIR([src/core/driver.c])
AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_CC
AC_PROG_RANLIB
AM_PROG_CC_C_O

dnl Checks for pg_config command.
AC_CHECK_PROGS(PGCONFIG, pg_config)

if test -z $PGCONFIG
then
  PGSQL_INCLUDE_DIR=/usr/local/pgsql/include
  PGSQL_LIB_DIR=/usr/local/pgsql/lib
else
  PGSQL_INCLUDE_DIR=`pg_config --includedir`
  PGSQL_LIB_DIR=`pg_config --libdir`
fi

AC_ARG_WITH(pgsql,
    [  --with-pgsql=DIR     site header files for PostgreSQL in DIR],
    [
        case "$withval" in
        "" | y | ye | yes | n | no)
            AC_MSG_ERROR([*** You must supply an argument to the --with-pgsql option.])
          ;;
        esac
        PGSQL_INCLUDE_DIR="$withval"/include
        PGSQL_LIB_DIR="$withval"/lib
    ])

AC_SUBST(PGSQL_INCLUDE_DIR)
AC_SUBST(PGSQL_LIB_DIR)

# Checks for libraries.
AC_CHECK_LIB([m], [pow])

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h string.h strings.h sys/time.h unistd.h math.h pthread.h semaphore.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_PID_T
AC_TYPE_SIZE_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([bzero gethostbyaddr gethostbyname gettimeofday socket strerror])

AC_DEFINE(STRICT_ASCII_STRING, 1, [strict string to use ascii only.])
AC_CONFIG_FILES([Makefile src/Makefile src/core/Makefile src/dbc/Makefile
			  src/dbc/pgsql/Makefile src/simple/Makefile src/include/Makefile])
AC_OUTPUT
