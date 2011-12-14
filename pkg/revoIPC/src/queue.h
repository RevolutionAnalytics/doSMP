/*****************************************************************
 * Shared task queue structure.
 *
 * This queue structure makes a few assumptions.  First, there will be only a
 * single master (i.e. only a single agent which is enqueuing tasks and
 * processing returned results at any given time).  This minimizes the required
 * synchronization.  It isn't essential that the master always be the same
 * process, but enqueue and wait_results operations may not safely run
 * concurrently with other enqueue or wait_results operations.
 *
 * dequeue and return_results are concurrency-safe.
 *****************************************************************/

#ifndef INCLUDED_TASKQUEUE_QUEUE_H
#define INCLUDED_TASKQUEUE_QUEUE_H

#include "boost/interprocess/managed_mapped_file.hpp"
#include "boost/interprocess/managed_shared_memory.hpp"
#include "boost/interprocess/sync/named_semaphore.hpp"
#include "boost/interprocess/sync/named_mutex.hpp"
#include "boost/interprocess/allocators/allocator.hpp"
#include "boost/interprocess/containers/string.hpp"

#ifndef WIN32
#if ! defined(_POSIX_SEMAPHORES) || ((_XOPEN_VERSION >= 600) && (_POSIX_SEMAPHORES - 0 <= 0))
#define RIPC_USE_SYSV_SEMAPHORES
#endif
#endif

#ifdef RIPC_USE_SYSV_SEMAPHORES
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#endif

using boost::interprocess::read_write;
using boost::interprocess::mapped_region;
using boost::interprocess::file_mapping;
using boost::interprocess::managed_mapped_file;
using boost::interprocess::managed_shared_memory;
using boost::interprocess::offset_ptr;
using boost::interprocess::named_semaphore;
using boost::interprocess::named_mutex;

namespace queue {

    typedef boost::interprocess::allocator<char, managed_shared_memory::segment_manager> 
                      CharAllocator;
    typedef boost::interprocess::basic_string<char,
            std::char_traits<char>,
            CharAllocator> shared_string_t;


    /* Exception thrown to indicate a requested shutdown. */
    class ShutdownException : public std::exception
    {
    public:
        ShutdownException() : std::exception() { }
        ~ShutdownException() throw() { }

        char const *what() const throw() { return "shutdown requested"; }
    };

    class MappedFile
    {
    public:
        MappedFile(std::string const &fname)
            : _file(fname.c_str(), read_write),
              _region(_file, read_write)
        { }

        ~MappedFile() { }

        void *get_address() { return _region.get_address(); }
        std::size_t get_size() { return _region.get_size(); }

    private:
        file_mapping    _file;
        mapped_region   _region;
    };

    /* Buffer to hold a single task/result. */
    class Message
    {
    public:

        /* Construct a new task buffer. */
        Message()
            : _next(NULL),
              _data(NULL),
              _id(0),
              _length(0),
              _refs(0)
        {
        }

        /* Get the id of this task. */
        int get_id() const { return _id; }

        /* Set the id of this task. */
        void set_id(int id) { _id = id; }

        /* Get the "next" pointer for this message in the current queue. */
        Message *get_next() const { return _next.get(); }

        /* Set the "next" pointer for this message in the current queue. */
        void set_next(Message *m) { _next = m; }

        /* Set the data for this message.  This is a raw pointer to data
         * allocated in the shared segment, and a raw length, which should be
         * negated if the data refers to a filename rather than the direct
         * data.
         */
        void set_data(char *msg, int len)
        {
            _data = msg;
            _length = len;
        }

        /* Clear the data for this message.  Caller is responsible for freeing
         * the memory in the shared segment.
         */
        void clear_data()
        {
            _data = NULL;
            _length = 0;
        }

        /* Get the "raw" length of this message.  Positive values indicate that
         * the data is stored in shared memory, and give the actual data
         * length.  Negative values indicate that the shared memory contains a
         * filename, and the length is the negation of the filename length.
         */
        inline int get_length() const { return _length; }

        /* Set the raw length of this message.  Caveat callor: this doesn't
         * change the allocation, so...  be wise.
         */
        inline void set_length(int len) { _length = len; }

        /* Get the "raw" data of this message.  If the length is negative, the
         * raw data actually contains a filename containing the data.
         */
        inline char *get_data() const { return _data.get(); }

    private:

        /* Pointer to next task in the current queue. */
        offset_ptr<Message>         _next;

        /* Pointer to the raw data for this message. */
        offset_ptr<char>            _data;

        /* Id for this task. */
        int                         _id;

        /* Raw length of this message. */
        int                         _length;

        /* Number of completions (broadcast message) */
        int                         _refs;
    };

    class QueueHandle;

    /* Handle to a message.  Message will be stored in shared memory, but
     * MessageHandle will not. */
    class MessageHandle
    {
    public:
        /* Create a new message handle. */
        MessageHandle(Message *msg,
                      QueueHandle *queue,
                      MessageHandle *env)
            : _message(msg),
              _queue(queue),
              _file_temp(NULL),
              _environment(env)
        {
        }

        /* Destructor for message handles. */
        ~MessageHandle() { }

        /* Get the id of this message. */
        int get_id() const { return _message->get_id(); }

        /* Set the data for this message.
         */
        void set_data(char const *msg, int len);

        /* Clear the data for this message, freeing the underlying resources.
         */
        void clear_data();

        /* Get the length of this message. */
        int get_length();

        /* Get the data of this message. */
        char *get_data();

        /* Release the data of this message.  This MUST be called after calling
         * get_data. */
        void release_data();

        /* Get the underlying message object. */
        Message *get_message() const { return _message; }

        /* Get the prevailing environment, if any. */
        MessageHandle *get_environment() const { return _environment; }

    private:
        /* Pointer to the message data itself. */
        Message            *_message;

        /* Pointer to the queue handle for this message. */
        QueueHandle        *_queue;

        /* Temporary pointer to a managed mapped file. */
        MappedFile         *_file_temp;

        /* The new environment, if one is required. */
        MessageHandle      *_environment;
    };

    /* The task queue.  Consists of an "outbound" task queue and an "inbound"
     * result queue.  enqueue/dequeue may be used to access the task queue, and
     * return_result/wait_result may be used to access toe result queue. */
    class Queue
    {
    public:
        /* Create a task queue, cleaning up any leftover resources from an old queue. */
        static Queue *create(managed_shared_memory &segment,
                             std::string const &name,
                             std::string const &tmpdir,
                             long               mem_size,
                             int                max_tasks,
                             int                file_thresh);

        /* Connect to the task queue. */
        static Queue *open(managed_shared_memory &segment);

        /* Get the name of this queue. */
        std::string get_name() const
        {
            return std::string(_name.begin(), _name.end());
        }

        /* Get the location of the temp dir for this queue. */
        std::string get_temp_dir() const
        {
            return std::string(_tmpdir.begin(), _tmpdir.end());
        }

        /* Get the total space in bytes available in task queue. */
        long get_mem_size() const { return _mem_size; }

        /* Get the maximum number of tasks in queue. */
        int get_max_tasks() const { return _max_tasks; }

        /* Threshold for sending data via file. */
        int get_file_threshold() const { return _file_threshold; }

        /* Get the number of outstanding tasks. */
        int get_num_outstanding_tasks() const { return _num_outstanding; }

        /* Initiate a shutdown of the task queue, signalling shutdown to the
         * workers.  Note that it may take a little while for the workers to
         * shutdown, so it's a good idea to delay briefly before destroying the
         * task queue.
         */
        void shutdown() { _shutdown = true; }

        /* Check if a shutdown has been requested for this task queue.
         */
        bool shutdown_signalled() { return _shutdown; }

        /* Allocate a task.  Once a task is allocated, it MUST be returned to
         * the queue using either 'enqueue', 'return_result', or
         * 'discard_result', or it will be leaked.
         */
        Message *allocate_task();

        /* Enqueue a task, appending it to the queue of tasks.  Returns the
         * unique id for this particular task. */
        int enqueue(Message *m);

        /* Dequeue the first available task, blocking until one becomes
         * available. */
        Message *dequeue();

        /* Return a result to the task queue. */
        void return_result(Message *msg);

        /* Dequeue the first available result. */
        Message *get_result();

        /* Discard a result.  This must be called upon the value returned from
         * wait_result to clean up the results after they have been digested.
         */
        void discard_result(Message *msg);

        /* Refresh the "environment" task. */
        Message *new_environment();

        /* Get the "environment" task. */
        Message *get_environment() const { return _environment.get(); }

        /* Get the current environment generation. */
        int get_environment_generation() const { return _env_generation; }

        /* Construct a new queue. (DO NOT USE.) */
        Queue(managed_shared_memory &segment,
              std::string const &name,
              std::string const &tmpdir,
              long mem_size,
              int buf_size,
              int file_thresh);

        /* Destroy a queue. (DO NOT USE.) */
        ~Queue();

    private:

        /* Next task id to allocate */
        int                             _next_id;

        /* Number of tasks which have been submitted, but whose results have
         * not been collected. */
        int                             _num_outstanding;

        /* Number of tasks which have been submitted, but which have not been
         * claimed by workers. */
        int                             _num_tasks;

        /* Number of results which have been returned, but which have not been
         * retrieved. */
        int                             _num_results;

        /* Number of results which are in the "free" queue -- i.e. which may be
         * allocated to hold new tasks.  This is the maximum number of new
         * tasks which can be immediately enqueued at this moment. */
        int                             _num_free;

        /* Generation counter for environments. */
        int                             _env_generation;

        /* Pointer to array of all task structures. */
        offset_ptr<Message>             _buffer;

        /* Head and tail pointer for task queue. */
        offset_ptr<Message>             _task_head;
        offset_ptr<Message>             _task_tail;

        /* Head and tail pointer for result queue. */
        offset_ptr<Message>             _result_head;
        offset_ptr<Message>             _result_tail;

        /* Linked list of free task objects. */
        offset_ptr<Message>             _free;

        /* Current environment. */
        offset_ptr<Message>             _environment;

        /* Name of this queue. */
        shared_string_t                 _name;

        /* Location of temp dir for this queue. */
        shared_string_t                 _tmpdir;

        /* Total space available in task queue. */
        long                            _mem_size;

        /* Maximum number of tasks in queue. */
        int                             _max_tasks;

        /* Threshold for sending data via file. */
        int                             _file_threshold;

        /* Shutdown flag */
        bool                            _shutdown;
    };

#ifdef WIN32
    typedef HANDLE SemaphoreType;
    typedef HANDLE MutexType;
#elif defined(RIPC_USE_SYSV_SEMAPHORES)
    typedef struct {
        int sem_handle;
        int sem_idx;
    } SemaphoreType;
    typedef SemaphoreType MutexType;
#else
    typedef named_semaphore *SemaphoreType;
    typedef named_mutex *MutexType;
#endif

    /* Handle to a task queue. */
    class QueueHandle
    {
    public:
        QueueHandle(std::string const &name);

        QueueHandle(std::string const &name,
                    std::string const &tmpdir,
                    long mem_size,
                    int  max_tasks,
                    int  inline_thresh);

        ~QueueHandle()
        {
#ifdef WIN32
            if (_task_count)
                ::CloseHandle(_task_count);
            if (_result_count)
                ::CloseHandle(_result_count);
            if (_lock)
                ::CloseHandle(_lock);
#elif defined(RIPC_USE_SYSV_SEMAPHORES)
            /* No cleanup needed. */
#else
            delete _task_count;
            delete _result_count;
            delete _lock;
#endif
            delete _segment;
            delete _environment;
        }

        /* Destroy a task queue, cleaning up any resources. */
        void destroy();

        /* Get the name of this queue. */
        std::string get_name() const
        {
            check_destroyed();
            return _queue->get_name();
        }

        /* Get the location of the temp dir for this queue. */
        std::string get_temp_dir() const
        {
            check_destroyed();
            return _queue->get_temp_dir();
        }

        /* Get the total space in bytes available in task queue. */
        long get_mem_size() const
        {
            check_destroyed();
            return _queue->get_mem_size();
        }

        /* Get the maximum number of tasks in queue. */
        int get_max_tasks() const
        {
            check_destroyed();
            return _queue->get_max_tasks();
        }

        /* Threshold for sending data via file. */
        int get_file_threshold() const
        {
            check_destroyed();
            return _queue->get_file_threshold();
        }

        /* Get the number of outstanding tasks. */
        int get_num_outstanding_tasks() const
        {
            check_destroyed();
            return _queue->get_num_outstanding_tasks();
        }

        /* Get raw access to the shared memory segment.  This may be used to
         * pass more complex data structures as part of a task message. */
        managed_shared_memory &segment()
        {
            check_destroyed();
            return * _segment;
        }

        /* Initiate a shutdown of the task queue, signalling shutdown to the
         * workers.  Note that it may take a little while for the workers to
         * shutdown, so it's a good idea to delay briefly before destroying the
         * task queue.
         */
        void shutdown();

        /* Allocate a task.  Once a task is allocated, it MUST be returned to
         * the queue using either 'enqueue', 'return_result', or
         * 'discard_result', or it will be leaked.
         */
        MessageHandle *allocate_task()
        {
            check_destroyed();
            return new MessageHandle(_queue->allocate_task(),
                                     this,
                                     NULL);
        }

        /* Enqueue a task, appending it to the queue of tasks.  Returns the
         * unique id for this particular task. */
        int enqueue(MessageHandle *m);

        /* Enqueue a task, allocating it, and copying its data from 'msg',
         * returning the task's unique id. */
        int enqueue(char const *msg, int length);

        /* Dequeue the first available task, blocking until one becomes
         * available. */
        MessageHandle *dequeue();

        /* Return a result to the task queue. */
        void return_result(MessageHandle *msg);

        /* Dequeue the first available result, returning NULL immediately if
         * none is available. */
        MessageHandle *check_result();

        /* Dequeue the first available result, blocking until one becomes
         * available. */
        MessageHandle *wait_result();

        /* Discard a result.  This must be called upon the value returned from
         * wait_result to clean up the results after they have been digested.
         */
        void discard_result(MessageHandle *msg)
        {
            check_destroyed();
            msg->clear_data();
            _queue->discard_result(msg->get_message());
            delete msg;
        }

        /* Add a new "environment" task. */
        void set_environment(char const *data, int length);

        /* Get the environment. */
        MessageHandle *get_environment();

    private:
        void check_destroyed() const
        {
            if (! _queue)
                throw ShutdownException();
        }

    private:

        /* The queue. */
        Queue                          *_queue;

        /* Semaphore containing available task count. */
        SemaphoreType                   _task_count;

        /* Semaphore containing available result count. */
        SemaphoreType                   _result_count;

        /* Semaphore used as a lock for the queues. */
        MutexType                       _lock;

        /* Shared memory segment containing the task queue. */
        managed_shared_memory          *_segment;

        /* Current generation number for environment tasks. */
        int                             _env_generation;

        /* Message handle for environment. */
        MessageHandle                  *_environment;
    };
}

#endif
