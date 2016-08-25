import multiprocessing
import signal
import pprint
import logging


def childkiller(signum, frame):
    print('Child Finishing.')
    exit(0)


def killer(signum, frame):
    import time
    print('\n\n-----> Request to shutdown received.\n\n')
    count = 0
    while True:
        time.sleep(0.2)
        count = count + 1
        try:
            print([process.is_alive() for process in processes])
        except:
            print('Brutal Ending !!')
            exit(1)
        if all([process.is_alive() for process in processes]):
            print('All process terminated.')
            exit(0)
        else:
            print("Still some children processes alive.")
        if count > 29:
            for process in processes:
                if process.is_alive():
                    process.terminate()
            exit(1)


def mp_wrapper(inqueue, outqueue, identity):
    signal.signal(signal.SIGINT, childkiller)
    import time
    counter = 0
    while True:
        counter += 1
        payload = inqueue.get()
        if (payload == "END"):
            outqueue.put((identity, "END"))
            return
        result = payload[0](*payload[1], **payload[2])
        outqueue.put(result)
        time.sleep(0.01)
    # creating children processes input queue list and process List


def mp_manager(func, args_list, threads_count=4):
    logger = logging.getLogger()
    signal.signal(signal.SIGINT, killer)
    logs = []
    global processes
    # preparing queues and process lists
    in_queues = list()
    processes = list()
    out_queue = multiprocessing.Queue()
    out_queue.cancel_join_thread
    # Staging Jobs jobs
    for count in range(threads_count):
        # creating in queues and puting them in queue list
        in_queues.append(multiprocessing.Queue())
        processes.append(multiprocessing.Process(target=mp_wrapper, args=(
            (in_queues[count]), out_queue, count,)))
    logger.debug('Starting Update %d Threads' % threads_count)
    # startring Jobs
    [processes[x].start() for x in range(threads_count)]
    logger.debug('Satrted Update %d Threads' % threads_count)
    # assigning subnets to queues
    for (index, args_data) in enumerate(args_list):
        in_queues[(index % threads_count)].put(
            (
                func,
                args_data['args'],
                args_data['kwargs'],
            )
        )
    logger.debug('Queue filled for  %d Threads' % threads_count)
    # marking the end of the queues
    [in_queues[x].put("END") for x in range(threads_count)]
    logger.debug('Queue poison pill sent for  %d Threads' % threads_count)
    result = []
    status = []
    while True:
        logger.debug('---- Received from output queue for Update:')
        data = out_queue.get()
        text = pprint.pformat(data, indent=4, width=80, depth=None)
        text = [' ' * 16 + x for x in text.split('\n')]
        logs.append(('debug', '\n'.join(text)))
        if type(data) is tuple:
            logger.debug('Process %s sent Poison pill.' % str(data[0]))
            logger.debug('Update result size is %d.' % len(result))
            status.append(data)
            logger.debug('Process End Status Size is %d.' % len(status))
            if len(status) == threads_count:
                break
        else:
            result.append(data)
            if 'logs' in data[data.keys()[0]]:
                for log in data[data.keys()[0]]['logs']:
                    logger.log(logging.getLevelName(log[0].upper()), log[1])
    logger.debug("UPdate Joinning Processes")
    return result
