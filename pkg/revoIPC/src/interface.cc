#include <unistd.h>

#include "queue.h"
#include "boost/interprocess/sync/named_mutex.hpp"
#include "boost/interprocess/sync/named_recursive_mutex.hpp"
#include "boost/interprocess/ipc/message_queue.hpp"

#include <cstdio>
// Oh, Windows...
#undef ERROR
#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

using queue::MessageHandle;

#define CHECK_TAG_TYPE

namespace {
    class invalid_cast
    {
    public:
        invalid_cast() throw() {}
        ~invalid_cast() throw() {}
    };

    int integer_value(SEXP scalar, int default_value=0)
    {
        if (TYPEOF(scalar) == INTSXP)
        {
            if (LENGTH(scalar) == 0)
                throw invalid_cast();
            return INTEGER(scalar)[0];
        }
        else if (TYPEOF(scalar) == REALSXP)
        {
            if (LENGTH(scalar) == 0)
                throw invalid_cast();
            return (int) (0.5 + REAL(scalar)[0]);
        }
        else if (TYPEOF(scalar) == NILSXP)
            return default_value;
        else
            throw invalid_cast();
    }

    template <typename T>
    SEXP _wrap_object(T *m, char const *tag_name)
    {
        if (m == NULL)
            return R_NilValue;

        SEXP tag = R_NilValue;
        SEXP prot = R_NilValue;
#ifdef CHECK_TYPE_TAG
        /* XXX: Need to protect tag? */
        PROTECT(tag = mkString(tag_name));
#else
        (void) tag_name;
#endif
        SEXP e = R_MakeExternalPtr(static_cast<void *>(m),
                                   tag,
                                   prot);
#ifdef CHECK_TYPE_TAG
        UNPROTECT(1);
#endif
        return e;
    }

    bool check_type_tag(SEXP task, char const *want_tag)
    {
#ifdef CHECK_TYPE_TAG
        SEXP tag = EXTPTR_TAG(task);
        if (! isString(tag))
            return false;
        tag = STRING_ELT(tag, 0);
        if (strcmp(CHAR(tag), want_tag))
            return false;
#else
        (void) task;
        (void) want_tag;
#endif
        return true;
    }

    template <typename T>
    T *_unwrap_object(SEXP s, char const *tag)
    {
        if (TYPEOF(s) != EXTPTRSXP)
            throw invalid_cast();

        if (check_type_tag(s, tag))
            return reinterpret_cast<T *>(EXTPTR_PTR(s));
        else
            throw invalid_cast();
    }

    template <typename T>
    struct Wrapper
    {
        static char const * const TAG_NAME;
        static char const * const DESCRIPTION;
        static R_CFinalizer_t FINALIZER;

        static SEXP wrap(T *obj)
        {
            SEXP s = _wrap_object(obj, TAG_NAME);
            R_RegisterCFinalizerEx(s, FINALIZER, TRUE);
            return s;
        }

        static T *unwrap(SEXP r_obj)
        {
            return _unwrap_object<T>(r_obj, TAG_NAME);
        }
    };

    template <typename T>
    SEXP wrap_object(T *obj)
    {
        return Wrapper<T>::wrap(obj);
    }

    SEXP wrap_result(MessageHandle *m)
    {
        return _wrap_object(m, "result");
    }

    SEXP wrap_task(MessageHandle *m)
    {
        return _wrap_object(m, "task");
    }

    char const *get_object_name(SEXP name)
    {
        if (TYPEOF(name) == STRSXP)
        {
            if (LENGTH(name) != 1)
                throw invalid_cast();
            name = STRING_ELT(name, 0);
        }
        if (TYPEOF(name) != CHARSXP)
            throw invalid_cast();
        else if (LENGTH(name) <= 0)
            throw invalid_cast();
    
        return CHAR(name);
    }

// =======================================================
// Task queue
// =======================================================

extern "C" SEXP q_release(SEXP sexp_q);
template<>
char const * const Wrapper<queue::QueueHandle>::TAG_NAME    = "queue";
template<>
char const * const Wrapper<queue::QueueHandle>::DESCRIPTION = "queue";
template<>
R_CFinalizer_t Wrapper<queue::QueueHandle>::FINALIZER = (R_CFinalizer_t) q_release;

extern "C"
SEXP q_create(SEXP sexp_name,
              SEXP sexp_tmpdir,
              SEXP sexp_memsize,
              SEXP sexp_maxtasks,
              SEXP sexp_filethresh)
{
    char const *name;
    char const *tmpdir;
    std::size_t mem_size = 0;
    std::size_t max_tasks = 0;
    std::size_t file_thresh = 0;

    try {
        name = get_object_name(sexp_name);
    }
    catch (invalid_cast ic)
    {
        error("Queue name must be a string value.");
        return R_NilValue;
    }

    try {
        tmpdir = get_object_name(sexp_tmpdir);
    }
    catch (invalid_cast ic)
    {
        error("Temporary directory must be a string value.");
        return R_NilValue;
    }

    try {
        mem_size = integer_value(sexp_memsize, 67108864);
    }
    catch (invalid_cast ic)
    {
        error("Shared memory size, if specified, must be a positive integer giving the size in bytes for the shared memory segment.");
        return R_NilValue;
    }

    try {
        max_tasks = integer_value(sexp_maxtasks, 256);
    }
    catch (invalid_cast ic)
    {
        error("Task queue length, if specified, must be a positive integer giving the number of task entries to allocate.");
        return R_NilValue;
    }

    try {
        file_thresh = integer_value(sexp_filethresh, 65536);
    }
    catch (invalid_cast ic)
    {
        error("File threshold, if specified, must be a positive integer giving the maximum size in bytes of data which may be passed in-line via shared memory.");
        return R_NilValue;
    }

    try {
        queue::QueueHandle *q = new queue::QueueHandle(name,
                                                       tmpdir,
                                                       mem_size,
                                                       max_tasks,
                                                       file_thresh);
        return wrap_object(q);
    }
    catch (boost::interprocess::interprocess_exception e)
    {
        return R_NilValue;
    }
}

extern "C"
SEXP q_open(SEXP sexp_name)
{
    char const *name;

    try {
        name = get_object_name(sexp_name);
    }
    catch (invalid_cast ic)
    {
        error("Queue name must be a string value.");
        return R_NilValue;
    }

    try {
        queue::QueueHandle *q = new queue::QueueHandle(name);
        return wrap_object(q);
    }
    catch (boost::interprocess::interprocess_exception e)
    {
        return R_NilValue;
    }
}

extern "C"
SEXP q_destroy(SEXP sexp_q)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q != NULL)
    {
        R_ClearExternalPtr(sexp_q);
        q->destroy();
        delete q;
        return ScalarLogical(1);
    }
    else
        return ScalarLogical(0);
}

extern "C"
SEXP q_release(SEXP sexp_q)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q != NULL)
    {
        R_ClearExternalPtr(sexp_q);
        delete q;
        return ScalarLogical(1);
    }
    else
        return ScalarLogical(0);
}

extern "C"
SEXP q_shutdown(SEXP sexp_q)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q == NULL)
    {
        error("The task queue has been freed.");
        return R_NilValue;
    }
    q->shutdown();
    return ScalarLogical(1);
}

extern "C"
SEXP q_enqueue(SEXP sexp_q, SEXP data)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q == NULL)
    {
        error("The task queue has been freed.");
        return R_NilValue;
    }
    else if (TYPEOF(data) != RAWSXP)
    {
        error("Only RAW (serialized) data may be added to a task queue.");
        return R_NilValue;
    }
    else
    {
        int id = q->enqueue(reinterpret_cast<char const *>(RAW(data)),
                            LENGTH(data));
        return ScalarInteger(id);
    }
}

extern "C"
SEXP q_dequeue(SEXP sexp_q)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q == NULL)
    {
        error("The task queue has been freed.");
        return R_NilValue;
    }

    MessageHandle *m;
    try {
        m = q->dequeue();
        return wrap_task(m);
    }
    catch(queue::ShutdownException se)
    {
        return R_NilValue;
    }
}

extern "C"
SEXP q_getTaskEnvironment(SEXP sexp_q, SEXP task)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q == NULL)
    {
        error("The task queue has been freed.");
        return R_NilValue;
    }
    if (TYPEOF(task) != EXTPTRSXP  ||
        ! check_type_tag(task, "task"))
    {
        error("The 'task' parameter must be a task returned from 'dequeue'.");
        return R_NilValue;
    }

    MessageHandle *m = reinterpret_cast<MessageHandle *>(EXTPTR_PTR(task));
    MessageHandle *env = m->get_environment();
    if (env == NULL)
        return R_NilValue;

    char *data = env->get_data();
    int length = env->get_length();
    SEXP ans;
    ans = allocVector(RAWSXP, length);
    /* Grr... */
    memcpy(RAW(ans), data, length);
    env->release_data();
    return ans;
}

extern "C"
SEXP q_getTaskData(SEXP sexp_q, SEXP task)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q == NULL)
    {
        error("The task queue has been freed.");
        return R_NilValue;
    }
    if (TYPEOF(task) != EXTPTRSXP  ||
        ! check_type_tag(task, "task"))
    {
        error("The 'task' parameter must be a task returned from 'dequeue'.");
        return R_NilValue;
    }

    MessageHandle *m = reinterpret_cast<MessageHandle *>(EXTPTR_PTR(task));
    char *data = m->get_data();
    int length = m->get_length();
    SEXP ans;
    ans = allocVector(RAWSXP, length);
    /* Grr... */
    memcpy(RAW(ans), data, length);
    return ans;
}

extern "C"
SEXP q_getId(SEXP sexp_q, SEXP task)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q == NULL)
    {
        error("The task queue has been freed.");
        return R_NilValue;
    }
    if (TYPEOF(task) != EXTPTRSXP  ||
        (! check_type_tag(task, "task") &&
         ! check_type_tag(task, "result")))
    {
        error("The 'task' parameter must be either a task object or a result object.");
        return R_NilValue;
    }

    MessageHandle *m = reinterpret_cast<MessageHandle *>(EXTPTR_PTR(task));
    return ScalarInteger(m->get_id());
}

extern "C"
SEXP q_returnResult(SEXP sexp_q, SEXP task, SEXP response)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q == NULL)
    {
        error("The task queue has been freed.");
        return R_NilValue;
    }
    if (TYPEOF(task) != EXTPTRSXP  ||
        ! check_type_tag(task, "task"))
    {
        error("The 'task' parameter must be a task returned from 'dequeue'.");
        return R_NilValue;
    }
    if (TYPEOF(response) != RAWSXP)
    {
        error("Only RAW (serialized) data may be returned as a task result.");
        return R_NilValue;
    }

    MessageHandle *m = reinterpret_cast<MessageHandle *>(EXTPTR_PTR(task));
    if (m == NULL)
    {
        error("A result has already been returned for this task.");
        return R_NilValue;
    }
    R_ClearExternalPtr(task);

    char const *data = reinterpret_cast<char const *>(RAW(response));
    int length = LENGTH(response);
    m->set_data(data, length);
    try {
        q->return_result(m);
        return ScalarLogical(1);
    }
    catch (queue::ShutdownException e)
    {
        return ScalarLogical(0);
    }
}

extern "C"
SEXP q_checkResult(SEXP sexp_q)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q == NULL)
    {
        error("The task queue has been freed.");
        return R_NilValue;
    }

    MessageHandle *m = q->check_result();
    if (m == NULL)
        return R_NilValue;

    return wrap_result(m);
}

extern "C"
SEXP q_waitResult(SEXP sexp_q)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q == NULL)
    {
        error("The task queue has been freed.");
        return R_NilValue;
    }

    MessageHandle *m = q->wait_result();
    if (m == NULL)
        return R_NilValue;

    return wrap_result(m);
}

extern "C"
SEXP q_getResultData(SEXP sexp_q, SEXP task)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q == NULL)
    {
        error("The task queue has been freed.");
        return R_NilValue;
    }
    if (TYPEOF(task) != EXTPTRSXP  ||
        ! check_type_tag(task, "result"))
    {
        error("The 'task' parameter must be a result object returned from a worker.");
        return R_NilValue;
    }

    MessageHandle *m = reinterpret_cast<MessageHandle *>(EXTPTR_PTR(task));
    char *data = m->get_data();
    int length = m->get_length();
    SEXP ans = allocVector(RAWSXP, length);
    /* Grr... */
    memcpy(RAW(ans), data, length);
    return ans;
}

extern "C"
SEXP q_discardResult(SEXP sexp_q, SEXP task)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q == NULL)
    {
        error("The task queue has been freed.");
        return R_NilValue;
    }
    if (TYPEOF(task) != EXTPTRSXP  ||
        ! check_type_tag(task, "result"))
    {
        error("The 'task' parameter must be a result object returned from a worker.");
        return R_NilValue;
    }

    MessageHandle *m = reinterpret_cast<MessageHandle *>(EXTPTR_PTR(task));
    if (m == NULL)
    {
        error("This result has already been discarded.");
        return R_NilValue;
    }
    R_ClearExternalPtr(task);

    q->discard_result(m);
    return ScalarLogical(1);
}

extern "C"
SEXP q_setEnvironment(SEXP sexp_q, SEXP sexp_task)
{
    queue::QueueHandle *q = Wrapper<queue::QueueHandle>::unwrap(sexp_q);
    if (q == NULL)
    {
        error("The task queue has been freed.");
        return R_NilValue;
    }
    if (TYPEOF(sexp_task) != RAWSXP)
    {
        error("Only RAW (serialized) data may be sent as a task environment.");
        return R_NilValue;
    }

    q->set_environment(reinterpret_cast<char const *>(RAW(sexp_task)),
                       LENGTH(sexp_task));
    return ScalarLogical(1);
}

extern "C"
SEXP q_fork(void)
{
#ifndef WIN32
    pid_t pid = fork();
    return ScalarInteger(pid);
#else
    return ScalarInteger(-1);
#endif
}

extern "C"
SEXP q_canFork(void)
{
#ifndef WIN32
    return ScalarInteger(1);
#else
    return ScalarInteger(0);
#endif
}

/////////////////////////////////////////////////////////////////////////////
// Semaphore
/////////////////////////////////////////////////////////////////////////////

namespace bi = boost::interprocess;
using boost::interprocess::create_only;
using boost::interprocess::open_only;
using boost::interprocess::open_or_create;
using boost::interprocess::interprocess_exception;

extern "C" SEXP ipc_sem_release(SEXP r_sem);
template<>
char const * const Wrapper<bi::named_semaphore>::TAG_NAME    = "semaphore";
template<>
char const * const Wrapper<bi::named_semaphore>::DESCRIPTION = "semaphore";
template<>
R_CFinalizer_t Wrapper<bi::named_semaphore>::FINALIZER = (R_CFinalizer_t) ipc_sem_release;

extern "C"
SEXP ipc_sem_create(SEXP r_name, SEXP r_initial)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Semaphore name must be a string giving the name of the semaphore.");
        return R_NilValue;
    }

    std::size_t initial = 0;
    try {
        initial = integer_value(r_initial);
    }
    catch (invalid_cast ic)
    {
        error("Semaphore initial count, if specified, must be an integer value giving the initial count for the semaphore.");
        return R_NilValue;
    }

    try {
        return wrap_object(new bi::named_semaphore(create_only, name, initial));
    }
    catch(interprocess_exception ie)
    {
        error("The semaphore \"%s\" could not be created: %s\n", name, ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_sem_open(SEXP r_name)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Semaphore name must be a string giving the name of the semaphore.");
        return R_NilValue;
    }

    try {
        return wrap_object(new bi::named_semaphore(open_only, name));
    }
    catch (interprocess_exception ie)
    {
        error("The semaphore \"%s\" was not found.", name);
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_sem_get(SEXP r_name, SEXP r_initial)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Semaphore name must be a string giving the name of the semaphore.");
        return R_NilValue;
    }

    std::size_t initial = 0;
    try {
        initial = integer_value(r_initial);
    }
    catch (invalid_cast ic)
    {
        error("Semaphore initial count, if specified, must be an integer value giving the initial count for the semaphore.");
        return R_NilValue;
    }

    try {
        return wrap_object(new bi::named_semaphore(open_or_create, name, initial));
    }
    catch(interprocess_exception ie)
    {
        error("The semaphore \"%s\" could not be opened: %s\n", name, ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_sem_release(SEXP r_sem)
{
    try {
        bi::named_semaphore *sem = Wrapper<bi::named_semaphore>::unwrap(r_sem);
        R_ClearExternalPtr(r_sem);
        delete sem;
        return R_NilValue;
    }
    catch (invalid_cast ic)
    {
        error("Object is not a semaphore.");
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_sem_destroy(SEXP r_name)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Semaphore name must be a string giving the name of the semaphore.");
        return R_NilValue;
    }

    if (bi::named_semaphore::remove(name))
        return ScalarLogical(1);
    else
        return ScalarLogical(0);
}

extern "C"
SEXP ipc_sem_wait(SEXP r_sem)
{
    try {
        bi::named_semaphore *sem = Wrapper<bi::named_semaphore>::unwrap(r_sem);
        if (sem == NULL)
        {
            error("The semaphore has been freed.");
            return R_NilValue;
        }
        sem->wait();
        return ScalarLogical(1);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a semaphore.");
        return R_NilValue;
    }
    catch (interprocess_exception ie)
    {
        error("Error while waiting on semaphore: %s\n", ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_sem_wait_try(SEXP r_sem)
{
    try {
        bi::named_semaphore *sem = Wrapper<bi::named_semaphore>::unwrap(r_sem);
        if (sem == NULL)
        {
            error("The semaphore has been freed.");
            return R_NilValue;
        }
        if (sem->try_wait())
            return ScalarLogical(1);
        return ScalarLogical(0);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a semaphore.");
        return R_NilValue;
    }
    catch (interprocess_exception ie)
    {
        error("Error while polling semaphore: %s\n", ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_sem_post(SEXP r_sem)
{
    try {
        bi::named_semaphore *sem = Wrapper<bi::named_semaphore>::unwrap(r_sem);
        if (sem == NULL)
        {
            error("The semaphore has been freed.");
            return R_NilValue;
        }
        sem->post();
        return ScalarLogical(1);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a semaphore.");
        return R_NilValue;
    }
    catch (interprocess_exception ie)
    {
        error("Error while posting to semaphore: %s\n", ie.what());
        return R_NilValue;
    }
}

/////////////////////////////////////////////////////////////////////////////
// Mutex
/////////////////////////////////////////////////////////////////////////////

extern "C" SEXP ipc_mutex_release(SEXP r_mutex);
template<>
char const * const Wrapper<bi::named_mutex>::TAG_NAME    = "mutex";
template<>
char const * const Wrapper<bi::named_mutex>::DESCRIPTION = "mutex";
template<>
R_CFinalizer_t Wrapper<bi::named_mutex>::FINALIZER = (R_CFinalizer_t) ipc_mutex_release;

extern "C"
SEXP ipc_mutex_create(SEXP r_name)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Mutex name must be a string giving the name of the mutex.");
        return R_NilValue;
    }

    try {
        return wrap_object(new bi::named_mutex(create_only, name));
    }
    catch (interprocess_exception ie)
    {
        error("The mutex \"%s\" could not be created: %s\n", name, ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mutex_open(SEXP r_name)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Mutex name must be a string giving the name of the mutex.");
        return R_NilValue;
    }

    try {
        return wrap_object(new bi::named_mutex(open_only, name));
    }
    catch (interprocess_exception ie)
    {
        error("The mutex \"%s\" was not found.", name);
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mutex_get(SEXP r_name, SEXP initial)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Mutex name must be a string giving the name of the mutex.");
        return R_NilValue;
    }

    try {
        return wrap_object(new bi::named_mutex(open_or_create, name));
    }
    catch (interprocess_exception ie)
    {
        error("The mutex \"%s\" could not be opened: %s\n", name, ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mutex_release(SEXP r_mutex)
{
    try {
        bi::named_mutex *mutex = Wrapper<bi::named_mutex>::unwrap(r_mutex);
        R_ClearExternalPtr(r_mutex);
        delete mutex;
        return R_NilValue;
    }
    catch (invalid_cast ie)
    {
        error("Object is not a mutex.");
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mutex_destroy(SEXP r_name)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Mutex name must be a string giving the name of the mutex.");
        return R_NilValue;
    }

    if (bi::named_mutex::remove(name))
        return ScalarLogical(1);
    else
        return ScalarLogical(0);
}

extern "C"
SEXP ipc_mutex_lock(SEXP r_mutex)
{
    try {
        bi::named_mutex *mutex = Wrapper<bi::named_mutex>::unwrap(r_mutex);
        if (mutex == NULL)
        {
            error("The mutex has been freed.");
            return R_NilValue;
        }
        mutex->lock();
        return ScalarLogical(1);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a mutex.");
        return R_NilValue;
    }
    catch (interprocess_exception ie)
    {
        error("Error while locking mutex: %s\n", ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mutex_lock_try(SEXP r_mutex)
{
    try {
        bi::named_mutex *mutex = Wrapper<bi::named_mutex>::unwrap(r_mutex);
        if (mutex == NULL)
        {
            error("The mutex has been freed.");
            return R_NilValue;
        }
        if (mutex->try_lock())
            return ScalarLogical(1);
        return ScalarLogical(0);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a mutex.");
        return R_NilValue;
    }
    catch (interprocess_exception ie)
    {
        error("Error while trying to lock mutex: %s\n", ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mutex_unlock(SEXP r_mutex)
{
    try {
        bi::named_mutex *mutex = Wrapper<bi::named_mutex>::unwrap(r_mutex);
        if (mutex == NULL)
        {
            error("The mutex has been freed.");
            return R_NilValue;
        }
        mutex->unlock();
        return ScalarLogical(1);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a mutex.");
        return R_NilValue;
    }
    catch (interprocess_exception ie)
    {
        error("Error while unlocking mutex: %s\n", ie.what());
        return R_NilValue;
    }
}

/////////////////////////////////////////////////////////////////////////////
// Recursive mutex
/////////////////////////////////////////////////////////////////////////////

extern "C" SEXP ipc_rmutex_release(SEXP r_mutex);
template<>
char const * const Wrapper<bi::named_recursive_mutex>::TAG_NAME    = "rmutex";
template<>
char const * const Wrapper<bi::named_recursive_mutex>::DESCRIPTION = "recursive mutex";
template<>
R_CFinalizer_t Wrapper<bi::named_recursive_mutex>::FINALIZER = (R_CFinalizer_t) ipc_rmutex_release;

extern "C"
SEXP ipc_rmutex_create(SEXP r_name, SEXP initial)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Recursive mutex name must be a string giving the name of the mutex.");
        return R_NilValue;
    }

    try {
        return wrap_object(new bi::named_recursive_mutex(create_only, name));
    }
    catch(interprocess_exception ie)
    {
        error("The recursive mutex \"%s\" could not be created: %s\n", name, ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_rmutex_open(SEXP r_name)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Recursive mutex name must be a string giving the name of the mutex.");
        return R_NilValue;
    }

    try {
        return wrap_object(new bi::named_recursive_mutex(open_only, name));
    }
    catch (interprocess_exception ie)
    {
        error("The recursive mutex \"%s\" was not found.", name);
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_rmutex_get(SEXP r_name, SEXP initial)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Recursive mutex name must be a string giving the name of the mutex.");
        return R_NilValue;
    }

    try {
        bi::named_recursive_mutex *mtx =
                    new bi::named_recursive_mutex(open_or_create, name);
        return wrap_object(mtx);
    }
    catch(interprocess_exception ie)
    {
        error("The recursive mutex \"%s\" could not be opened: %s\n", name, ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_rmutex_release(SEXP r_mutex)
{
    try {
        bi::named_recursive_mutex *mutex = Wrapper<bi::named_recursive_mutex>::unwrap(r_mutex);
        R_ClearExternalPtr(r_mutex);
        delete mutex;
        return R_NilValue;
    }
    catch (invalid_cast ie)
    {
        error("Object is not a recursive mutex.");
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_rmutex_destroy(SEXP r_name)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Recursive mutex name must be a string giving the name of the mutex.");
        return R_NilValue;
    }

    if (bi::named_recursive_mutex::remove(name))
        return ScalarLogical(1);
    else
        return ScalarLogical(0);
}

extern "C"
SEXP ipc_rmutex_lock(SEXP r_mutex)
{
    try {
        bi::named_recursive_mutex *mutex = Wrapper<bi::named_recursive_mutex>::unwrap(r_mutex);
        if (mutex == NULL)
        {
            error("The mutex has been freed.");
            return R_NilValue;
        }
        mutex->lock();
        return ScalarLogical(1);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a recursive mutex.");
        return R_NilValue;
    }
    catch (interprocess_exception ie)
    {
        error("Error while locking recursive mutex: %s\n", ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_rmutex_lock_try(SEXP r_mutex)
{
    try {
        bi::named_recursive_mutex *mutex = Wrapper<bi::named_recursive_mutex>::unwrap(r_mutex);
        if (mutex == NULL)
        {
            error("The mutex has been freed.");
            return R_NilValue;
        }
        if (mutex->try_lock())
            return ScalarLogical(1);
        return ScalarLogical(0);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a recursive mutex.");
        return R_NilValue;
    }
    catch (interprocess_exception ie)
    {
        error("Error while trying to lock recursive mutex: %s\n", ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_rmutex_unlock(SEXP r_mutex)
{
    try {
        bi::named_recursive_mutex *mutex = Wrapper<bi::named_recursive_mutex>::unwrap(r_mutex);
        if (mutex == NULL)
        {
            error("The mutex has been freed.");
            return R_NilValue;
        }
        mutex->unlock();
        return ScalarLogical(1);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a recursive mutex.");
        return R_NilValue;
    }
    catch (interprocess_exception ie)
    {
        error("Error while unlocking recursive mutex: %s\n", ie.what());
        return R_NilValue;
    }
}

/////////////////////////////////////////////////////////////////////////////
// Message queue
/////////////////////////////////////////////////////////////////////////////

extern "C" SEXP ipc_mqueue_release(SEXP r_mq);
template<>
char const * const Wrapper<bi::message_queue>::TAG_NAME    = "mqueue";
template<>
char const * const Wrapper<bi::message_queue>::DESCRIPTION = "message queue";
template<>
R_CFinalizer_t Wrapper<bi::message_queue>::FINALIZER = (R_CFinalizer_t) ipc_mqueue_release;

extern "C"
SEXP ipc_mqueue_create(SEXP r_name, SEXP r_max_msg, SEXP r_max_msg_size)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Message queue name must be a string giving the name of the queue.");
        return R_NilValue;
    }

    std::size_t max_msg = 0;
    std::size_t max_size = 0;

    try {
        max_msg = integer_value(r_max_msg);
    }
    catch (invalid_cast ic)
    {
        error("Message queue max messages, if specified, must be an integer value giving the maximum number of messages in the message queue at once.");
        return R_NilValue;
    }

    try {
        max_size = integer_value(r_max_msg_size);
    }
    catch (invalid_cast ic)
    {
        error("Message queue max message size, if specified, must be an integer value giving the maximum size of any message in the message queue.");
        return R_NilValue;
    }

    if (max_msg == 0)
        max_msg = 128;
    if (max_size == 0)
        max_size = 16384;

    try {
        return wrap_object(new bi::message_queue(create_only,
                                                 name,
                                                 max_msg,
                                                 max_size));
    }
    catch(interprocess_exception ie)
    {
        error("The message queue \"%s\" could not be created: %s\n", name, ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mqueue_open(SEXP r_name)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Message queue name must be a string giving the name of the queue.");
        return R_NilValue;
    }

    try {
        return wrap_object(new bi::message_queue(open_only, name));
    }
    catch (interprocess_exception ie)
    {
        error("The message queue \"%s\" was not found.", name);
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mqueue_get(SEXP r_name, SEXP r_max_msg, SEXP r_max_msg_size)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Message queue name must be a string giving the name of the queue.");
        return R_NilValue;
    }

    std::size_t max_msg = 0;
    std::size_t max_size = 0;

    try {
        max_msg = integer_value(r_max_msg);
    }
    catch (invalid_cast ic)
    {
        error("Message queue max messages, if specified, must be an integer value giving the maximum number of messages in the message queue at once.");
        return R_NilValue;
    }

    try {
        max_size = integer_value(r_max_msg_size);
    }
    catch (invalid_cast ic)
    {
        error("Message queue max message size, if specified, must be an integer value giving the maximum size of any message in the message queue.");
        return R_NilValue;
    }

    if (max_msg == 0)
        max_msg = 128;
    if (max_size == 0)
        max_size = 16384;

    try {
        return wrap_object(new bi::message_queue(open_or_create,
                                                 name,
                                                 max_msg,
                                                 max_size));
    }
    catch(interprocess_exception ie)
    {
        error("The message queue \"%s\" could not be opened: %s\n", name, ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mqueue_release(SEXP r_mq)
{
    try {
        bi::message_queue *mq = Wrapper<bi::message_queue>::unwrap(r_mq);
        R_ClearExternalPtr(r_mq);
        delete mq;
    }
    catch (invalid_cast ie)
    {
        error("Object is not a message queue.");
        return R_NilValue;
    }
    return R_NilValue;
}

extern "C"
SEXP ipc_mqueue_destroy(SEXP r_name)
{
    char const *name;
    try {
        name = get_object_name(r_name);
    }
    catch (invalid_cast ic)
    {
        error("Message queue name must be a string giving the name of the queue.");
        return R_NilValue;
    }

    bi::message_queue::remove(name);
    return ScalarLogical(1);
}

extern "C"
SEXP ipc_mqueue_send(SEXP r_mq, SEXP r_message, SEXP r_prio)
{
    // Get message data
    Rbyte *data = NULL;
    if (TYPEOF(r_message) != RAWSXP)
    {
        error("Message sent to a message queue must be 'raw' data.");
        return R_NilValue;
    }
    else
        data = RAW(r_message);

    // Get message length
    std::size_t length = LENGTH(r_message);

    // Get message priority
    std::size_t prio;
    try {
        prio = integer_value(r_prio);
    }
    catch (invalid_cast ie)
    {
        error("Message priority, if specified, must be an integer value giving the priority with which to send the message.");
        return R_NilValue;
    }

    // Send it
    try {
        bi::message_queue *mq = Wrapper<bi::message_queue>::unwrap(r_mq);
        if (mq == NULL)
        {
            error("The message queue has been freed.");
            return R_NilValue;
        }
        mq->send(data, length, prio);
        return ScalarLogical(1);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a message queue.");
        return R_NilValue;
    }
    catch (interprocess_exception ie)
    {
        error("Error while sending to message queue: %s", ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mqueue_send_try(SEXP r_mq, SEXP r_message, SEXP r_prio)
{
    // Get message data
    Rbyte *data = NULL;
    if (TYPEOF(r_message) != RAWSXP)
    {
        error("Message sent to a message queue must be 'raw' data.");
        return R_NilValue;
    }
    else
        data = RAW(r_message);

    // Get message length
    std::size_t length = LENGTH(r_message);

    // Get message priority
    std::size_t prio;
    try {
        prio = integer_value(r_prio);
    }
    catch (invalid_cast ie)
    {
        error("Message priority, if specified, must be an integer value giving the priority with which to send the message.");
        return R_NilValue;
    }

    // Send it
    try {
        bi::message_queue *mq = Wrapper<bi::message_queue>::unwrap(r_mq);
        if (mq == NULL)
        {
            error("The message queue has been freed.");
            return R_NilValue;
        }
        if (mq->try_send(data, length, prio))
            return ScalarLogical(1);
        else
            return ScalarLogical(0);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a message queue.");
        return R_NilValue;
    }
    catch (interprocess_exception ie)
    {
        error("Error while sending to message queue: %s", ie.what());
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mqueue_receive(SEXP r_mq)
{
    bi::message_queue *mq;
    try {
        mq = Wrapper<bi::message_queue>::unwrap(r_mq);
        if (mq == NULL)
        {
            error("The message queue has been freed.");
            return R_NilValue;
        }
    }
    catch (invalid_cast ie)
    {
        error("Object is not a message queue.");
        return R_NilValue;
    }

    std::size_t max_size = mq->get_max_msg_size();
    char *buffer = new char[max_size];
    try {
        std::size_t actual_size = 0;
        unsigned int priority = 0;
        mq->receive(buffer, max_size, actual_size, priority);

        SEXP r_message = allocVector(RAWSXP, actual_size);
        memcpy(RAW(r_message), buffer, actual_size);
        delete[] buffer;
        return r_message;
    }
    catch (interprocess_exception ie)
    {
        error("Error while receiving from message queue: %s", ie.what());
        delete[] buffer;
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mqueue_receive_try(SEXP r_mq)
{
    bi::message_queue *mq;
    try {
        mq = Wrapper<bi::message_queue>::unwrap(r_mq);
        if (mq == NULL)
        {
            error("The message queue has been freed.");
            return R_NilValue;
        }
    }
    catch (invalid_cast ie)
    {
        error("Object is not a message queue.");
        return R_NilValue;
    }

    std::size_t max_size = mq->get_max_msg_size();
    char *buffer = new char[max_size];
    try {
        std::size_t actual_size = 0;
        unsigned int priority = 0;
        if (! mq->try_receive(buffer, max_size, actual_size, priority))
        {
            delete[] buffer;
            return R_NilValue;
        }

        SEXP r_message = allocVector(RAWSXP, actual_size);
        memcpy(RAW(r_message), buffer, actual_size);
        delete[] buffer;
        return r_message;
    }
    catch (interprocess_exception ie)
    {
        error("Error while receiving from message queue: %s", ie.what());
        delete[] buffer;
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mqueue_get_max_msgs(SEXP r_mq)
{
    try {
        bi::message_queue *mq = Wrapper<bi::message_queue>::unwrap(r_mq);
        if (mq == NULL)
        {
            error("The message queue has been freed.");
            return R_NilValue;
        }
        std::size_t max_msgs = mq->get_max_msg();
        return ScalarInteger(max_msgs);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a message queue.");
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mqueue_get_max_size(SEXP r_mq)
{
    try {
        bi::message_queue *mq = Wrapper<bi::message_queue>::unwrap(r_mq);
        if (mq == NULL)
        {
            error("The message queue has been freed.");
            return R_NilValue;
        }
        std::size_t max_msg = mq->get_max_msg_size();
        return ScalarInteger(max_msg);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a message queue.");
        return R_NilValue;
    }
}

extern "C"
SEXP ipc_mqueue_get_num_msgs(SEXP r_mq)
{
    try {
        bi::message_queue *mq = Wrapper<bi::message_queue>::unwrap(r_mq);
        if (mq == NULL)
        {
            error("The message queue has been freed.");
            return R_NilValue;
        }
        std::size_t num_msgs = mq->get_num_msg();
        return ScalarInteger(num_msgs);
    }
    catch (invalid_cast ie)
    {
        error("Object is not a message queue.");
        return R_NilValue;
    }
}

};

R_CallMethodDef callMethods[] = {
    {"create",          (void *(*)()) & q_create,                   5},
    {"open",            (void *(*)()) & q_open,                     1},
    {"shutdown",        (void *(*)()) & q_shutdown,                 1},
    {"release",         (void *(*)()) & q_release,                  1},
    {"destroy",         (void *(*)()) & q_destroy,                  1},
    {"enqueue",         (void *(*)()) & q_enqueue,                  2},
    {"dequeue",         (void *(*)()) & q_dequeue,                  1},
    {"getId",           (void *(*)()) & q_getId,                    2},
    {"getTaskData",     (void *(*)()) & q_getTaskData,              2},
    {"getTaskEnv",      (void *(*)()) & q_getTaskEnvironment,       2},
    {"returnResult",    (void *(*)()) & q_returnResult,             3},
    {"checkResult",     (void *(*)()) & q_checkResult,              1},
    {"waitResult",      (void *(*)()) & q_waitResult,               1},
    {"getResultData",   (void *(*)()) & q_getResultData,            2},
    {"discardResult",   (void *(*)()) & q_discardResult,            2},
    {"setEnvironment",  (void *(*)()) & q_setEnvironment,           2},
    {"fork",            (void *(*)()) & q_fork,                     0},
    {"canFork",         (void *(*)()) & q_canFork,                  0},

    {"sem_create",      (void *(*)()) & ipc_sem_create,             2},
    {"sem_open",        (void *(*)()) & ipc_sem_open,               1},
    {"sem_get",         (void *(*)()) & ipc_sem_get,                2},
    {"sem_release",     (void *(*)()) & ipc_sem_release,            1},
    {"sem_destroy",     (void *(*)()) & ipc_sem_destroy,            1},
    {"sem_wait",        (void *(*)()) & ipc_sem_wait,               1},
    {"sem_waitTry",     (void *(*)()) & ipc_sem_wait_try,           1},
    {"sem_post",        (void *(*)()) & ipc_sem_post,               1},

    {"mutex_create",    (void *(*)()) & ipc_mutex_create,           1},
    {"mutex_open",      (void *(*)()) & ipc_mutex_open,             1},
    {"mutex_get",       (void *(*)()) & ipc_mutex_get,              1},
    {"mutex_release",   (void *(*)()) & ipc_mutex_release,          1},
    {"mutex_destroy",   (void *(*)()) & ipc_mutex_destroy,          1},
    {"mutex_lock",      (void *(*)()) & ipc_mutex_lock,             1},
    {"mutex_lockTry",   (void *(*)()) & ipc_mutex_lock_try,         1},
    {"mutex_unlock",    (void *(*)()) & ipc_mutex_unlock,           1},

    {"rmutex_create",   (void *(*)()) & ipc_rmutex_create,          1},
    {"rmutex_open",     (void *(*)()) & ipc_rmutex_open,            1},
    {"rmutex_get",      (void *(*)()) & ipc_rmutex_get,             1},
    {"rmutex_release",  (void *(*)()) & ipc_rmutex_release,         1},
    {"rmutex_destroy",  (void *(*)()) & ipc_rmutex_destroy,         1},
    {"rmutex_lock",     (void *(*)()) & ipc_rmutex_lock,            1},
    {"rmutex_lockTry",  (void *(*)()) & ipc_rmutex_lock_try,        1},
    {"rmutex_unlock",   (void *(*)()) & ipc_rmutex_unlock,          1},

    /* XXX upgradeable mutex */

    {"mq_create",       (void *(*)()) & ipc_mqueue_create,          3},
    {"mq_open",         (void *(*)()) & ipc_mqueue_open,            1},
    {"mq_get",          (void *(*)()) & ipc_mqueue_get,             3},
    {"mq_release",      (void *(*)()) & ipc_mqueue_release,         1},
    {"mq_destroy",      (void *(*)()) & ipc_mqueue_destroy,         1},
    {"mq_send",         (void *(*)()) & ipc_mqueue_send,            3},
    {"mq_sendTry",      (void *(*)()) & ipc_mqueue_send_try,        3},
    {"mq_receive",      (void *(*)()) & ipc_mqueue_receive,         1},
    {"mq_receiveTry",   (void *(*)()) & ipc_mqueue_receive_try,     1},
    {"mq_getMaxMsgs",   (void *(*)()) & ipc_mqueue_get_max_msgs,    1},
    {"mq_getNumMsgs",   (void *(*)()) & ipc_mqueue_get_num_msgs,    1},
    {"mq_getMaxSize",   (void *(*)()) & ipc_mqueue_get_max_size,    1},

    {NULL, NULL, 0}
};

extern "C"
void R_init_revoIPC(DllInfo *info)
{
    (void) info;
    R_registerRoutines(info, NULL, callMethods, NULL, NULL);
}

extern "C"
void R_unload_revoIPC(DllInfo *info)
{
    (void) info;
}

