//
// Created by Avi Kogan on 25/04/2022.
//

#include "MapReduceFramework.h"
#include "Barrier.h"

#define JOB_STATE_PERCENTAGE_INIT 0

using namespace std;

typedef std::vector<IntermediateVec> ShuffleVec;

/** */
// todo class
struct ThreadContext {
    // Input vec handled
    int threadID;


    Barrier* endMapBarrier;

    // Atomic counter - Place in InputVec - type 1 -> type 2 ++, type 2 -> t3 : -- or init

    // Atomic counter - How many calls for emit2 : for percentage in shuffle.

    // Atomic counter - number of out elements
    std::atomic<int>* atomic_counter; // Atomic counter - Place in InputVec.
                                      // ++ in type 1 ->  type 2 | type 2 -> t3 : need init first

    const MapReduceClient *client; // give access to map and reduce client's functions.

    InputVec *inputVec; // Input vec - for accessing in the map stage. (according to
    OutputVec *outputVec;
    IntermediateVec *pairs;

    ShuffleVec *shuffleVec;

    JobState *js;

};

/** */
// todo class
struct JobContext{

//public:
//    JobContext(already_joined, js, multiThreadLevel, threads, t_contexts, mutex)
    JobState *js;
    int multiThreadLevel;
    pthread_t *threads;
    ThreadContext *threads_context;

    pthread_mutex_t *waitForJob_Mutex;
};


// vec -> pairs of map stage for each thread.
// queue of vecs -> for the shuffle in the JobContext.


// -------------------- emit functions -------------------//

void emit2 (K2* key, V2* value, void* context){
    IntermediateVec* pairs =  static_cast<const ThreadContext*>(context)->pairs)

    // q.push(K2, V2) // push the context

};


void emit3 (K3* key, V3* value, void* context){

    // IntermediateVec* pairs =  (IntermediateVec * ) context

    // update of number of out elements - atomic var
};


// ---------------- thread entry point ---------------- //

void* thread_entry_point(void* arg)
{
    auto tc = (ThreadContext*) arg;
    tc->js->stage = MAP_STAGE;

    // iterate over input and fill intermediate:

    //    for (int i = 0; i < 1000; ++i) {
    //        // old_value isn't used in this example, but will be necessary
    //        // in the exercise
    //        int old_value = (*(tc->atomic_counter))++;
    //        (void) old_value;  // ignore not used warning
    //        (*(tc->bad_counter))++;
    //    }

    // ------ endMapBarrier ------

    // if thread == 0
        // lock()

        // tc->js = SHUFFLE_STAGE
        // do shuffle

        // end_lock()
    // else:
    // ------ cv of shuffleWait ---
    // ------ shuffleWait ------
    //      locked until 0 broadcast all after shuffle.
    //      sem_wait();

    // ------ start of reduce ------

    // tc->js = REDUCE_STAGE

    // iterate over intermedate and fill out:
    // cv + semaphore - up to
    //    for (int i = 0; i < 1000; ++i) {
    //        // old_value isn't used in this example, but will be necessary
    //        // in the exercise
    //        int old_value = (*(tc->atomic_counter))++;
    //        (void) old_value;  // ignore not used warning
    //        (*(tc->bad_counter))++;
    //    }

    // pthread exit??

}

// ---------------- shuffle func ---------------- //


// ---------------- startMapReduceJob ---------------- //


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){

    try{
        std::atomic<int> atomic_counter(0);

        JobState *js = new JobState{UNDEFINED_STAGE, JOB_STATE_PERCENTAGE_INIT};

        // create #multiThreadLevel threads array
        auto *threads = new pthread_t[multiThreadLevel]; // todo new failed?
        ThreadContext *t_contexts = new ThreadContext[multiThreadLevel];  // todo new failed?


        // todo make_unique()?
        // todo unique_ptr is enough?
        unique_ptr<JobContext> job = unique_ptr<JobContext>(new JobContext{js, multiThreadLevel,
                                                                           threads, t_contexts,
                                                                           PTHREAD_MUTEX_INITIALIZER});

        Barrier *barrier = new Barrier(multiThreadLevel); // create the endMapBarrier object todo is deleted ?

        for (int i = 0; i < multiThreadLevel; ++i) {
            // give access to inputVec + outputVec + endMapBarrier
            // for each thread give access to initialized atomic_counter.
            t_contexts[i] = {i, &barrier, atomic_counter, client, inputVec, outputVec};
        }


        for (int i = 0; i < multiThreadLevel; ++i) {
            pthread_create(threads + i, nullptr, thread_entry_point, t_contexts + i);
        }

        return job.get();

    }catch (bad_alloc& exp){
        fprintf(stderr, "Alloc failed"); // todo
        exit(EXIT_FAILURE); // todo exit value
    }


}


// ----------------- Job functions -------------------- //

void waitForJob(JobHandle job){

    JobContext *cur_job = static_cast<JobContext*>(job);

    pthread_mutex_lock(cur_job->waitForJob_Mutex);

    for (int i = 0; i < cur_job->multiThreadLevel; ++i) {
        pthread_join(cur_job->threads[i], NULL);
    }

    pthread_mutex_unlock(cur_job->waitForJob_Mutex);

    // after the mutex unlocked - all threads terminated, so according to pthread_join manual
    // calling pthread_join() on terminated thread it returns immediately.
    // so no need for other conditions.
}


void getJobState(JobHandle job, JobState* state){
    state = static_cast<JobContext *>(job)->js;
}


void closeJobHandle(JobHandle job){
    // todo
    // free resources
    // delete job;
    int a = 0;
}