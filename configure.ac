#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([sqlbench], [0.6.1], [dianchengwang@gmail.com])
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
	  PGSQL_LIBS="-L${PGSQL_LIB_DIR} -lpq"
          ;;
	*)
        PGSQL_INCLUDE_DIR="$withval/include"
        PGSQL_LIB_DIR="$withval/lib"
	PGSQL_LIBS="-L${PGSQL_LIB_DIR} -lpq"
	;;
        esac
	postgresql=yes
	AC_SUBST(PGSQL_INCLUDE_DIR)
	AC_SUBST(PGSQL_LIB_DIR)
	AC_SUBST(PGSQL_LIBS)
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
	  MYSQL_LIB_DIR=`$MYSQL_CONFIG --variable=pkglibdir`
	  MYSQL_LIBS=`$MYSQL_CONFIG --libs`
	  ;;
	  *)
	  MYSQL_INCLUDE_DIR="-I${withval}/include"
	  MYSQL_LIB_DIR="${withval}/lib"
	  MYSQL_LIBS=-L"${MYSQL_LIB_DIR} -lmysqlclient -lm"
	  ;;
	esac
	mysql=yes
	AC_SUBST(MYSQL_INCLUDE_DIR)
	AC_SUBST(MYSQL_LIB_DIR)
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
        KINGBASE_LIB_DIR="${withval}/lib"
        KINGBASE_LIBS="-L${KINGBASE_LIB_DIR} -lkci"
	AC_SUBST(KINGBASE_INCLUDE_DIR)
	AC_SUBST(KINGBASE_LIB_DIR)
	AC_SUBST(KINGBASE_LIBS)
    ])

AM_CONDITIONAL([ENABLE_KINGBASE], [test "$kingbase" = yes])
if test "$kingbase" = yes
then
	AC_DEFINE(ENABLE_KINGBASE, 1, [enable kingbase support.])
fi

odbc=no
AC_ARG_WITH(odbc,
    [  --with-odbc=yes     enable UnixODBC support],
    [
        case "$withval" in
	"" | y | ye | yes)
	    AC_CHECK_HEADERS(sql.h,
	        [AC_EGREP_HEADER([SQLExecDirect], sql.h, [],
		[AC_MSG_ERROR([header file <sql.h> does not match UnixODBC library])])],
		[AC_MSG_ERROR([header file <sql.h> is required for UnixODBC])])
          ;;
	n | no)
	  ;;
	*)
		AC_MSG_ERROR([*** you can supply yes or no to the --with-odbc option.])
	;;
        esac
	odbc=yes
	ODBC_LIBS=-lodbc
	AC_SUBST(ODBC_LIBS)
    ])

AM_CONDITIONAL([ENABLE_ODBC], [test "$odbc" = yes])
if test "$odbc" = yes
then
	AC_DEFINE(ENABLE_ODBC, 1, [enable odbc support.])
fi

if test "$postgresql" != yes -a "$kingbase" != yes -a "$mysql" != yes -a "$odbc" != yes
then
	AC_MSG_ERROR([*** you must select database client interface using --with-PACKAGE options, current supported: postgresql, mysql, kingbase and odbc. ])
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
		src/storeproc/mysql/Makefile
		src/scripts/mysql/Makefile
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

if test "$odbc" = yes
then
	AC_CONFIG_FILES([
		src/dbc/odbc/Makefile
	])
fi

AC_OUTPUT
