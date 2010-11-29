AC_INIT(revoIPC, 0.1, jed@revolution-computing.com)
AC_PREREQ([2.13])

AC_PROG_CC

PKG_LIBS=''
AC_CHECK_LIB(rt, shm_open, [
     PKG_LIBS="${PKG_LIBS} -lrt"
             ], [])
AC_SUBST(PKG_LIBS)

AC_CONFIG_FILES([src/Makevars])
AC_OUTPUT
