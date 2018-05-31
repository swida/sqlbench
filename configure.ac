#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([sqlbench], [0.6.0], [dianchengwang@gmail.com])
AM_INIT_AUTOMAKE([silent-rules])
AC_CONFIG_SRCDIR([src/core/driver.c])
AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_CC
AC_PROG_RANLIB
AM_PROG_CC_C_O

postgresql=no
AC_ARG_WITH(postgresql,
    [  --with-postgresql=yes|DIR     site header files for PostgreSQL in DIR],
    [
        case "$withval" in
        "" | y | ye | yes)
	  AC_CHECK_PROGS(PG_CONFIG, pg_config)

	  if test -z "$PG_CONFIG"
	  then
		AC_MSG_ERROR([*** Command pg_config not found, you can supply the site path to the --with-postgresql option.])
	  fi

	  PGSQL_INCLUDE_DIR=`$PG_CONFIG --includedir`
	  PGSQL_LIB_DIR=`$PG_CONFIG --libdir`
          ;;
	*)
        PGSQL_INCLUDE_DIR="$withval/include"
        PGSQL_LIB_DIR="$withval/lib"
	;;
        esac
	postgresql=yes
	AC_SUBST(PGSQL_INCLUDE_DIR)
	AC_SUBST(PGSQL_LIB_DIR)
    ])

AM_CONDITIONAL([ENABLE_POSTGRESQL], [test "$postgresql" = yes])

if test "$postgresql" = yes
then
	AC_DEFINE(ENABLE_POSTGRESQL, 1, [enable postgresql support.])
fi

mysql=no
AC_ARG_WITH(mysql,
    [  --with-mysql=yes|DIR     enable mysql support],
    [
        case "$withval" in
        ""| y | ye | yes)
	  AC_CHECK_PROGS(MYSQL_CONFIG, mysql_config)
	  if test -z "$MYSQL_CONFIG"
	  then
		AC_MSG_ERROR([*** Command mysql_config not found, you can supply the site path to the --with-mysql option.])
	  fi
	  MYSQL_INCLUDE_DIR=`$MYSQL_CONFIG --include`
	  MYSQL_LIBS=`$MYSQL_CONFIG --libs`
	  ;;
	  *)
	  MYSQL_INCLUDE_DIR="-I${withval}/include"
	  MYSQL_LIBS=-L"${withval}/lib -lmysqlclient"
	  ;;
	esac
	mysql=yes
	AC_SUBST(MYSQL_INCLUDE_DIR)
	AC_SUBST(MYSQL_LIBS)
    ])

AM_CONDITIONAL([ENABLE_MYSQL], [test "$mysql" = yes])

if test "$mysql" = yes
then
	AC_DEFINE(ENABLE_MYSQL, 1, [enable mysql support.])
fi

kingbase=no
AC_ARG_WITH(kingbase,
    [  --with-kingbase=DIR     site header and library files for Kingbase in DIR],
    [
        case "$withval" in
        y | ye | yes)
            AC_MSG_ERROR([*** You must supply an argument to the --with-kingbase option.])
          ;;
        esac
	kingbase=yes
        KINGBASE_INCLUDE_DIR="${withval}/include"
        KINGBASE_LIBS=-L"${withval}/lib -lkci"
	AC_SUBST(KINGBASE_INCLUDE_DIR)
	AC_SUBST(KINGBASE_LIBS)
    ])

AM_CONDITIONAL([ENABLE_KINGBASE], [test "$kingbase" = yes])
if test "$kingbase" = yes
then
	AC_DEFINE(ENABLE_KINGBASE, 1, [enable kingbase support.])
fi

if test "$postgresql" != yes -a "$kingbase" != yes -a "$mysql" != yes
then
	AC_MSG_ERROR([*** you must select database using --with-DATABASE options, current supported DATABASE: postgresql, mysql or kingbase. ])
fi
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

AC_CONFIG_FILES(
	[Makefile
	src/Makefile
	src/core/Makefile
	src/dbc/Makefile
	src/simple/Makefile
	src/extended/Makefile
	src/storeproc/Makefile
	src/utils/Makefile
	src/scripts/Makefile
	src/include/Makefile])

if test "$postgresql" = yes
then
	AC_CONFIG_FILES([
		src/dbc/pgsql/Makefile
		src/storeproc/pgsql/Makefile
		src/storeproc/pgsql/c/Makefile
		src/storeproc/pgsql/plpgsql/Makefile
		src/scripts/pgsql/Makefile
	])
fi

if test "$mysql" = yes
then
	AC_CONFIG_FILES([
		src/dbc/mysql/Makefile
	])
fi

if test "$kingbase" = yes
then
	AC_CONFIG_FILES([
		src/dbc/kingbase/Makefile
		src/storeproc/kingbase/Makefile
		src/storeproc/kingbase/plsql/Makefile
	])
fi

AC_OUTPUT
