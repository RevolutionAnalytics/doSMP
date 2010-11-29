#include "queue.h"
#include <fstream>
#include <exception>
#include <cassert>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>
#ifdef WIN32
#include <winbase.h>
#include <io.h>
#include <fcntl.h>
#include <share.h>
#include <direct.h>
#else
#include <dirent.h>
#include <unistd.h>
#endif

using boost::interprocess::mapped_region;
using boost::interprocess::file_mapping;
using boost::interprocess::shared_memory_object;
using boost::interprocess::create_only;
using boost::interprocess::open_only;
using boost::interprocess::open_or_create;

#ifdef WIN32
#define RIPC_ACQUIRE_MUTEX(mtx)  ::WaitForSingleObject((mtx), INFINITE)
#define RIPC_RELEASE_MUTEX(mtx)  ::ReleaseMutex((mtx))

#define RIPC_WAIT_SEMAPHORE(sem) ::WaitForSingleObject((sem), INFINITE)
#define RIPC_POLL_SEMAPHORE(sem) (0 == ::WaitForSingleObject((sem), 0))
#define RIPC_POST_SEMAPHORE(sem) ::ReleaseSemaphore((sem), 1, NULL);
#elif defined(RIPC_USE_SYSV_SEMAPHORES)

#ifdef _SEM_SEMUN_UNDEFINED
union semun {
    int              val;    /* Value for SETVAL */
    struct semid_ds *buf;    /* Buffer for IPC_STAT, IPC_SET */
    unsigned short  *array;  /* Array for GETALL, SETALL */
};
#endif

static void sysv_sem_wait(queue::SemaphoreType *mtx)
{
    struct sembuf ctl_buf = { mtx->sem_idx, -1, SEM_UNDO };
    semop(mtx->sem_handle, & ctl_buf, 1);
}

static bool sysv_sem_poll(queue::SemaphoreType *mtx)
{
    struct sembuf ctl_buf = { mtx->sem_idx, -1, SEM_UNDO | IPC_NOWAIT };
    bool success = (semop(mtx->sem_handle, & ctl_buf, 1) != -1);
    return success;
}

static void sysv_sem_post(queue::SemaphoreType *mtx)
{
    struct sembuf ctl_buf = { mtx->sem_idx, 1, SEM_UNDO };
    semop(mtx->sem_handle, & ctl_buf, 1);
}

#define RIPC_ACQUIRE_MUTEX(mtx)  sysv_sem_wait(& (mtx))
#define RIPC_RELEASE_MUTEX(mtx)  sysv_sem_post(& (mtx))

#define RIPC_WAIT_SEMAPHORE(sem) sysv_sem_wait(& (sem))
#define RIPC_POLL_SEMAPHORE(sem) sysv_sem_poll(& (sem))
#define RIPC_POST_SEMAPHORE(sem) sysv_sem_post(& (sem))
#else
#define RIPC_ACQUIRE_MUTEX(mtx) (mtx)->lock()
#define RIPC_RELEASE_MUTEX(mtx) (mtx)->unlock()

#define RIPC_WAIT_SEMAPHORE(sem) (sem)->wait()
#define RIPC_POLL_SEMAPHORE(sem) (sem)->try_wait()
#define RIPC_POST_SEMAPHORE(sem) (sem)->post()
#endif

namespace {

    bool is_directory(std::string const &dir)
    {
#ifdef WIN32
        WIN32_FIND_DATA ffd;    // Directory traversal state
        HANDLE sh = FindFirstFile(dir.c_str(), &ffd);
        if (INVALID_HANDLE_VALUE == sh)
            return false;
        bool is_dir = (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
        FindClose(sh);
        return is_dir;
#else
        struct stat sb;
        if (stat(dir.c_str(), &sb) == -1)
            return false;
        return S_ISDIR(sb.st_mode);
#endif
    }

    /* Destroy a directory and all of its subdirectories. */
    void nuke(std::string const &dir)
    {
#ifdef WIN32
        std::string searchPath(dir + "\\*");

        WIN32_FIND_DATA ffd; // file information struct
        HANDLE sh = FindFirstFile(searchPath.c_str(), &ffd);
        if (INVALID_HANDLE_VALUE == sh)
            return;

        do {
            if (strcmp(ffd.cFileName, ".") == 0  ||
                strcmp(ffd.cFileName, "..") == 0)
                continue;

            std::string fullPath(dir + '\\' + ffd.cFileName);
            if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
                nuke(fullPath);
            else
                std::remove(fullPath.c_str());
        } while (FindNextFile(sh, &ffd));

        FindClose(sh);
#else
        DIR *d = opendir(dir.c_str());
        if (d == NULL)
            return;

        struct dirent *de = readdir(d);
        /* XXX: Use readdir_r if it is available. */
        do {
            std::string path(dir + '/' + de->d_name);
            if (is_directory(path))
            {
                if (de->d_name[0] == '.')
                {
                    if (de->d_name[1] == '\0'  ||
                        (de->d_name[1] == '.'  &&  de->d_name[2] == '\0'))
                        continue;
                }
                nuke(path);
            }
            else
                std::remove(path.c_str());
        }
        while ((de = readdir(d)) != NULL);
        closedir(d);
        std::remove(dir.c_str());
#endif
    }

    /* Need to add portable code for making a directory and any non-existent
     * parent directories. */
    void mkdirs(std::string const &dir)
    {
        if (is_directory(dir))
            return;

        /* XXX */
#ifdef WIN32
        _mkdir(dir.c_str());
#else
        mkdir(dir.c_str(), 0700);
#endif
    }

    /* Create a temporary file, optionally pre-populating it with specific
     * contents. */
    std::string make_temp_file(int id, std::string const &dir, char const *data, int len)
    {
        char buffer[100];
        sprintf(buffer, "%08d", id);
        std::string filepath(dir + '/' + buffer);

        std::filebuf fbuf;
        fbuf.open(filepath.c_str(),
                    std::ios_base::in    | std::ios_base::out 
                  | std::ios_base::trunc | std::ios_base::binary); 

        // Set the size
        fbuf.pubseekoff(len - 1, std::ios_base::beg);
        fbuf.sputc(0);
        fbuf.close();

        // If we have data, copy it in
        if (data != NULL)
        {
            file_mapping m_file(filepath.c_str(),  read_write);
            mapped_region region(m_file, read_write);
            memcpy(region.get_address(), data, len);
        }

        return filepath;
    }

    /* Get the length of a file in bytes. */
    int get_file_length(std::string const &filename)
    {
        FILE *f = fopen(filename.c_str(), "rb");
        if (f == NULL)
            return 0;

        fseek(f, 0, SEEK_END);
        int len = ftell(f);
        fclose(f);
        return len;
    }

    /* Segment details */
    char const SEGMENT_NAME[]               = "(Shm)";

    /* Semaphores */
    char const SEMAPHORE_NAME_NUMTASKS[]    = "(SemNumTasks)";
    char const SEMAPHORE_NAME_NUMREPLIES[]  = "(SemNumReplies)";
    char const MUTEX_NAME_LOCK[]            = "(MtxLock)";

    /* Buffer details */
    char const BUFFER_NAME[]                = "QueueBuf";
};

namespace queue {

    void MessageHandle::set_data(char const *msg, int len)
    {
        /* Free the old data.
           NOTE: This means we cannot copy the response data directly out of
                 the task data.  Insofar as this is used from R, this isn't a
                 problem, as we can't directly use the data in the task buffer
                 anyway, but must deserialize it.
        */
        clear_data();

        /* If the data is below the threshold, try to stash it in the shared
         * memory directly. */
        if (len <= _queue->get_file_threshold())
        {
            try {
                /* Allocate the new buffer and copy over the data. */
                char *new_d = static_cast<char *>(_queue->segment().allocate(len));
                memcpy(new_d, msg, len);

                /* Now put in the new data. */
                _message->set_data(new_d, len);
                return;
            }
            catch (std::bad_alloc b)
            {
                /* Recovery: we'll fail over to mmapped files. */
            }
        }

        /* Copy the data to a file and stash its filename in the shared memory.
         * XXX: This could be an epic fail if we don't even have enough room in
         *      the shared memory for the filename.  Hopefully it won't come to
         *      that... */
        std::string fname = make_temp_file(_message->get_id(),
                                           _queue->get_temp_dir(),
                                           msg, len);
        char *data = static_cast<char *>(_queue->segment().allocate(fname.size() + 1));
        memcpy(data, fname.c_str(), fname.size() + 1);
        _message->set_data(data, - (int) (1 + fname.size()));
    }

    void MessageHandle::clear_data(void)
    {
        if (_message->get_data())
        {
            /* _file_temp != NULL -> file stored in mmapped file. */
            if (_file_temp)
            {
                delete _file_temp;
                _file_temp = NULL;
                std::string path(_message->get_data());
                std::remove(path.c_str());
            }

            _queue->segment().deallocate(_message->get_data());
            _message->set_data(NULL, 0);
        }
    }

    int MessageHandle::get_length()
    {
        int const len = _message->get_length();

        /* Length >= 0 is the actual data length. */
        if (len >= 0)
            return len;

        /* Length < 0 means we get the length from a file. */
        return get_file_length(_message->get_data());
    }

    char *MessageHandle::get_data()
    {
        int const len = _message->get_length();

        /* Length >= 0 means the data is in shared memory. */
        if (len >= 0)
            return _message->get_data();

        /* Otherwise, let's map the file containing the data and return a
         * pointer to it. */
        if (_file_temp == NULL)
            _file_temp = new MappedFile(_message->get_data());
        return reinterpret_cast<char *>(_file_temp->get_address());
    }

    void MessageHandle::release_data()
    {
        if (_file_temp != NULL)
        {
            delete _file_temp;
            _file_temp = NULL;
        }
    }

    Queue *Queue::create(managed_shared_memory &segment,
                         std::string const &name,
                         std::string const &tmpdir,
                         long mem_size,
                         int max_tasks,
                         int file_thresh)
    {
        /* Construct the task queue in the shared memory segment. */
        return segment.construct<Queue>(BUFFER_NAME)(segment,
                                                     name,
                                                     tmpdir,
                                                     mem_size,
                                                     max_tasks,
                                                     file_thresh);
    }

    Queue *Queue::open(managed_shared_memory &segment)
    {
        /* Find the task queue in the shared memory segment. */
        std::pair<Queue *, std::size_t> res = segment.find<Queue>(BUFFER_NAME);
        Queue *q = res.first;
        return q;
    }

    Queue::Queue(managed_shared_memory &segment,
                 std::string const &name,
                 std::string const &tmpdir,
                 long mem_size,
                 int max_tasks,
                 int file_thresh)
        : _next_id(1000),
          _num_outstanding(0),
          _num_tasks(0),
          _num_results(0),
          _num_free(max_tasks),
          _env_generation(0),
          _buffer(),
          _task_head(),
          _task_tail(),
          _result_head(),
          _result_tail(),
          _free(),
          _environment(NULL),
          _name(name.c_str(), CharAllocator(segment.get_segment_manager())),
          _tmpdir(tmpdir.c_str(), CharAllocator(segment.get_segment_manager())),
          _mem_size(mem_size),
          _max_tasks(max_tasks),
          _file_threshold(file_thresh),
          _shutdown(false)
    {
        /* Allocate the task structures, and link them together into the free
         * queue. */
        _buffer = segment.construct<Message>(boost::interprocess::anonymous_instance)[max_tasks]();
        for (int i=0; i<max_tasks - 1; ++i)
            _buffer[i].set_next(&_buffer[i+1]);
        _free = &_buffer[0];
    }

    Queue::~Queue()
    {
    }

    Message *Queue::allocate_task(void)
    {
        /* If our free queue is empty, we cannot allocate another task. */
        if (! _free)
            return NULL;

        /* Remove empty task structure from the free list. */
        Message *msgbuf = _free.get();
        _free = msgbuf->get_next();
        msgbuf->set_next(NULL);
        -- _num_free;

        /* Allocate id */
        msgbuf->set_id(_next_id ++);
        return msgbuf;
    }

    int Queue::enqueue(Message *m)
    {
        int id = m->get_id();
        if (! _task_head)
            _task_head = m;
        else
            _task_tail->set_next(m);
        _task_tail = m;
        ++ _num_tasks;
        ++ _num_outstanding;
        return id;
    }

    Message *Queue::dequeue(void)
    {
        Message *msg = _task_head.get();
        -- _num_tasks;
        _task_head = msg->get_next();
        if (! _task_head)
            _task_tail = NULL;
        msg->set_next(NULL);
        return msg;
    }

    void Queue::return_result(Message *msg)
    {
        msg->set_next(NULL);
        if (! _result_head)
            _result_head = msg;
        else
            _result_tail->set_next(msg);
        _result_tail = msg;
        ++ _num_results;
    }

    Message *Queue::get_result()
    {
        Message *msg = _result_head.get();
        _result_head = msg->get_next();
        if (! _result_head)
            _result_tail = NULL;
        msg->set_next(NULL);
        -- _num_results;
        -- _num_outstanding;
        return msg;
    }

    void Queue::discard_result(Message *msg)
    {
        /* Free the contents of this task structure and add it to the free
         * list. */
        msg->clear_data();
        msg->set_next(_free.get());
        _free = msg;
        ++ _num_free;
    }

    Message *Queue::new_environment()
    {
        Message *m = _environment.get();
        if (m == NULL)
            _environment = m = allocate_task();
        ++ _env_generation;
        return m;
    }

    QueueHandle::QueueHandle(std::string const &name)
      : _queue(NULL),
#if defined(RIPC_USE_SYSV_SEMAPHORES)
        _task_count(),
        _result_count(),
        _lock(),
#else
        _task_count(NULL),
        _result_count(NULL),
        _lock(NULL),
#endif
        _segment(NULL),
        _env_generation(-1),
        _environment(NULL)
    {
        /* Find our shared memory. */
        _segment      = new managed_shared_memory(open_only, (name + SEGMENT_NAME).c_str());

        /* Find the task queue in the shared memory segment. */
        _queue        = Queue::open(*_segment);

        /* Find our semaphores/mutexes. */
#ifdef WIN32
        _task_count   = ::OpenSemaphoreA(SEMAPHORE_ALL_ACCESS, FALSE, (name + SEMAPHORE_NAME_NUMTASKS).c_str());
        _result_count = ::OpenSemaphoreA(SEMAPHORE_ALL_ACCESS, FALSE, (name + SEMAPHORE_NAME_NUMREPLIES).c_str());
        _lock         = ::OpenMutexA(SYNCHRONIZE, FALSE, (name + MUTEX_NAME_LOCK).c_str());
#elif defined(RIPC_USE_SYSV_SEMAPHORES)
        std::string sem_file(_queue->get_temp_dir() + "/" + name + "(semaphores)");
        key_t semkey = ftok(sem_file.c_str(), 'R');
        int handle = semget(semkey, 0, 0600);
        if (handle == -1)
        {
            /* XXX: Raise the alarm!  You fail at semaphores! */
        }
        _task_count.sem_handle   = handle;
        _task_count.sem_idx      = 0;
        _result_count.sem_handle = handle;
        _result_count.sem_idx    = 1;
        _lock.sem_handle         = handle;
        _lock.sem_idx            = 2;
#else
        _task_count   = new named_semaphore(open_only, (name + SEMAPHORE_NAME_NUMTASKS).c_str());
        _result_count = new named_semaphore(open_only, (name + SEMAPHORE_NAME_NUMREPLIES).c_str());
        _lock         = new named_mutex(open_only, (name + MUTEX_NAME_LOCK).c_str());
#endif
    }

    QueueHandle::QueueHandle(std::string const &name,
                             std::string const &tmpdir,
                             long mem_size,
                             int  max_tasks,
                             int  inline_thresh)
      : _queue(NULL),
#if defined(RIPC_USE_SYSV_SEMAPHORES)
        _task_count(),
        _result_count(),
        _lock(),
#else
        _task_count(NULL),
        _result_count(NULL),
        _lock(NULL),
#endif
        _segment(NULL),
        _env_generation(-1),
        _environment(NULL)
    {
        /* Create a directory for our mmapped files. */
        mkdirs(tmpdir.c_str());

        /* Create our shared memory and semaphore resources. */
#ifdef WIN32
        _task_count   = ::CreateSemaphoreA(NULL, 0, INT_MAX, (name + SEMAPHORE_NAME_NUMTASKS).c_str());
        _result_count = ::CreateSemaphoreA(NULL, 0, INT_MAX, (name + SEMAPHORE_NAME_NUMREPLIES).c_str());
        _lock         = ::CreateMutexA(NULL, FALSE, (name + MUTEX_NAME_LOCK).c_str());
#elif defined(RIPC_USE_SYSV_SEMAPHORES)
        std::string sem_file(tmpdir + "/" + name + "(semaphores)");
        ::close(::open(sem_file.c_str(), O_CREAT | O_TRUNC, 0600));
        key_t semkey = ftok(sem_file.c_str(), 'R');
        int handle = semget(semkey, 3, IPC_CREAT | IPC_EXCL | 0600);
        if (handle == -1)
        {
            /* XXX: Raise the alarm!  You fail at semaphores! */
        }
        static unsigned short values[] = { 0, 0, 1 };
        union semun arg;
        arg.array = values;
        semctl(handle, 0, SETALL, arg);
        _task_count.sem_handle   = handle;
        _task_count.sem_idx      = 0;
        _result_count.sem_handle = handle;
        _result_count.sem_idx    = 1;
        _lock.sem_handle         = handle;
        _lock.sem_idx            = 2;
#else
        _task_count   = new named_semaphore(create_only, (name + SEMAPHORE_NAME_NUMTASKS).c_str(),   0);
        _result_count = new named_semaphore(create_only, (name + SEMAPHORE_NAME_NUMREPLIES).c_str(), 0);
        _lock         = new named_mutex(create_only,     (name + MUTEX_NAME_LOCK).c_str());
#endif
        _segment      = new managed_shared_memory(create_only, (name + SEGMENT_NAME).c_str(), mem_size);

        /* Construct the task queue in the shared memory segment. */
        _queue        = Queue::create(*_segment,
                                      name,
                                      tmpdir,
                                      mem_size,
                                      max_tasks,
                                      inline_thresh);
    }

    void QueueHandle::destroy()
    {
        /* In case it hasn't been shutdown, do so now. */
        if (! _queue)
            return;
        shutdown();

        /* Release our shared memory. */
        std::string name(_queue->get_name());
        std::string tmpdir(_queue->get_temp_dir());
        delete _segment;
        _queue = NULL;
        _segment = NULL;
        _environment = NULL;

        /* Destroy the semaphores. */
#ifdef WIN32
        if (_task_count)
            ::CloseHandle(_task_count);
        if (_result_count)
            ::CloseHandle(_result_count);
        if (_lock)
            ::CloseHandle(_lock);
#elif defined(RIPC_USE_SYSV_SEMAPHORES)
        semctl(_task_count.sem_handle, 0, IPC_RMID, 0);
#else
        try { named_semaphore::remove((name + SEMAPHORE_NAME_NUMTASKS).c_str());   } catch (...) { }
        try { named_semaphore::remove((name + SEMAPHORE_NAME_NUMREPLIES).c_str()); } catch (...) { }
        try { named_mutex::remove((name + MUTEX_NAME_LOCK).c_str());               } catch (...) { }
#endif

        /* Destroy the shared memory segment. */
        try { shared_memory_object::remove((name + SEGMENT_NAME).c_str()); } catch (...) { }

        /* Destroy the mmapped files directory. */
        try { nuke(tmpdir); } catch (...) { }

        /* Be sanitary: NULL our pointers. */
#if defined(RIPC_USE_SYSV_SEMAPHORES)
        _task_count.sem_handle   = -1;
        _task_count.sem_idx      = -1;
        _result_count.sem_handle = -1;
        _result_count.sem_idx    = -1;
        _lock.sem_handle         = -1;
        _lock.sem_idx            = -1;
#else
        _task_count = _result_count = NULL;
        _lock = NULL;
#endif
    }

    void QueueHandle::shutdown()
    {
        check_destroyed();

        /* Acquire lock, set shutdown flag, release lock. */
        RIPC_ACQUIRE_MUTEX(_lock);
        _queue->shutdown();
        RIPC_RELEASE_MUTEX(_lock);

        /* Signal the task queue to wake any waiters. */
        RIPC_POST_SEMAPHORE(_task_count);
    }

    int QueueHandle::enqueue(MessageHandle *m)
    {
        check_destroyed();

        /* Lock, add to task queue, unlock. */
        RIPC_ACQUIRE_MUTEX(_lock);
        int id = _queue->enqueue(m->get_message());
        RIPC_RELEASE_MUTEX(_lock);

        /* Post task notification to the semaphore */
        RIPC_POST_SEMAPHORE(_task_count);
        return id;
    }

    int QueueHandle::enqueue(char const *msg, int length)
    {
        check_destroyed();

        /* Allocate an empty task. */
        MessageHandle *m = allocate_task();

        /* Populate task. */
        m->set_data(msg, length);

        /* Enqueue the task. */
        int id = enqueue(m);
        delete m;
        return id;
    }

    MessageHandle *QueueHandle::dequeue(void)
    {
        check_destroyed();

        /* Wait for a task to become available.  Claim it atomically.  Because
         * the semaphore has been signalled, and because we acquired the
         * semaphore successfully, we can be certain that either there will be
         * a task waiting for us immediately, or that a shutdown has been
         * signalled. */
        RIPC_WAIT_SEMAPHORE(_task_count);

        /* Lock, remove next task from queue, unlock. */
        RIPC_ACQUIRE_MUTEX(_lock);
        if (_queue->shutdown_signalled())
        {
            /* Post to the task count again to wake the next waiter, if there
             * are any. */
            RIPC_POST_SEMAPHORE(_task_count);
            RIPC_RELEASE_MUTEX(_lock);
            throw ShutdownException();
        }

        /* If there is a new prevailing environment, fetch it. */
        MessageHandle *env = NULL;
        int cur_generation = _queue->get_environment_generation();
        if (_env_generation != cur_generation)
        {
            env = get_environment();
            _env_generation = cur_generation;
        }

        /* Dequeue the message itself. */
        Message *msg = _queue->dequeue();
        RIPC_RELEASE_MUTEX(_lock);

        return new MessageHandle(msg, this, env);
    }

    void QueueHandle::return_result(MessageHandle *msg)
    {
        check_destroyed();

        /* Lock, append to results queue, unlock. */
        RIPC_ACQUIRE_MUTEX(_lock);
        if (_queue->shutdown_signalled())
        {
            RIPC_POST_SEMAPHORE(_task_count);
            RIPC_RELEASE_MUTEX(_lock);
            throw ShutdownException();
        }
        _queue->return_result(msg->get_message());
        RIPC_RELEASE_MUTEX(_lock);

        /* Post result notification to the semaphore. */
        RIPC_POST_SEMAPHORE(_result_count);
        delete msg;
    }

    MessageHandle *QueueHandle::check_result()
    {
        check_destroyed();

        /* If we have no outstanding tasks, there is no result to retrieve.
         * Fail fast.  No locking required because we assume that only the
         * master will update this counter. */
        if (_queue->get_num_outstanding_tasks() <= 0)
            return NULL;

        /* Block for result return. */
        if (! RIPC_POLL_SEMAPHORE(_result_count))
            return NULL;

        /* Lock, append to results queue, unlock. */
        RIPC_ACQUIRE_MUTEX(_lock);
        Message *msg = _queue->get_result();
        RIPC_RELEASE_MUTEX(_lock);

        return new MessageHandle(msg, this, NULL);
    }

    MessageHandle *QueueHandle::wait_result()
    {
        check_destroyed();

        /* If we have no outstanding tasks, there is no result to retrieve.
         * Fail fast.  No locking required because we assume that only the
         * master will update this counter. */
        if (_queue->get_num_outstanding_tasks() <= 0)
            return NULL;

        /* Block for result return. */
        RIPC_WAIT_SEMAPHORE(_result_count);

        /* Lock, append to results queue, unlock. */
        RIPC_ACQUIRE_MUTEX(_lock);
        Message *msg = _queue->get_result();
        RIPC_RELEASE_MUTEX(_lock);

        return new MessageHandle(msg, this, NULL);
    }

    void QueueHandle::set_environment(char const *data, int length)
    {
        check_destroyed();
        RIPC_ACQUIRE_MUTEX(_lock);
        Message *env = _queue->new_environment();
        if (_environment == NULL)
            _environment = new MessageHandle(env, this, NULL);
        _environment->set_data(data, length);
        RIPC_RELEASE_MUTEX(_lock);
    }

    MessageHandle *QueueHandle::get_environment()
    {
        if (_environment == NULL)
        {
            Message *env = _queue->get_environment();
            if (env != NULL)
                _environment = new MessageHandle(env, this, NULL);
        }
        return _environment;
    }
};
