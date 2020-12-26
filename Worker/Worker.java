/*
A util class that divides tasks into n subtasks. The main task is to
 process objects in the form of linked list storage. To realize real
 task processing in IWork interface, IListWork interface is to
 process the small linked list after segmentation, IAtomicWork is to
 process each object in the linked list separately. In IListWork,
 addCurrentWorkedCount method must be called manually once in each
 loop, otherwise getCurrentWorkedCount will get an incorrect value

Author: Jia.Liu Data:2020/12/27
 */

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Worker<E> {
    public static int MAX_THREAD_SIZE = 100;
    public static int MODE_ATOMIC_WORK = 0;
    public static int MODE_LIST_WORK = 1;
    private Builder<?> mBuilder;
    private AtomicLong mCurrentWorkedCount;
    private ExecutorService mExecutor;
    private CountDownLatch mCompletedCount;
    private boolean mIsCanceled;

    private Worker(Builder<?> builder) {
        this.mBuilder = builder;
        mCompletedCount = new CountDownLatch(mBuilder.mTotalThreadSize);
        mExecutor = Executors.newFixedThreadPool(mBuilder.mTotalThreadSize + 1);
        mCurrentWorkedCount = new AtomicLong(0);
        mIsCanceled = false;
    }

    public long getCurrentWorkedCount() {
        return mCurrentWorkedCount.get();
    }

    public void addCurrentWorkedCount() {
        mCurrentWorkedCount.addAndGet(1);
    }

    public void await() {
        try {
            mCompletedCount.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void await(long time, TimeUnit unit) {
        try {
            mCompletedCount.await(time, unit);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void cancel() {
        mIsCanceled = true;
        mExecutor.shutdownNow();
        allCountDown();
    }

    public void start() {
        for (int i = 0; i < mBuilder.mTotalThreadSize; i++) {
            mExecutor.execute(getRunner(i));
        }
        mExecutor.execute(() -> {
            try {
                mCompletedCount.await();
                mBuilder.mCallback.onComplete();
                shutDown();
            } catch (InterruptedException e) {

            }
        });
    }

    public boolean isCanceled() {
        return mIsCanceled;
    }

    private Runnable getRunner(int workId) {
        return new Runnable() {
            @Override
            public void run() {
                List<?> work = getWork(workId);
                if (mBuilder.mCallback != null) {
                    if (mBuilder.mMode == MODE_ATOMIC_WORK) {
                        if (mBuilder.mCallback instanceof IAtomicWork) {
                            IAtomicWork callback = (IAtomicWork) mBuilder.mCallback;
                            int singleWorkSize = getSingleWorkSize(workId);
                            for (int i = 0; i < singleWorkSize && !mIsCanceled; i++) {
                                callback.doWork(work.get(i));
                                addCurrentWorkedCount();
                            }
                            mCompletedCount.countDown();
                        } else {
                            throw new IllegalArgumentException();
                        }
                    } else if (mBuilder.mMode == MODE_LIST_WORK) {
                        if (mBuilder.mCallback instanceof IListWork) {
                            ((IListWork) mBuilder.mCallback).doWork(work, getSingleWorkSize(workId));
                            mCurrentWorkedCount.addAndGet(getSingleWorkSize(workId));
                            mCompletedCount.countDown();
                        } else {
                            throw new IllegalArgumentException();
                        }
                    } else {
                        throw new IllegalStateException();
                    }
                } else {
                    throw new IllegalStateException("call back is null");
                }
            }
        };
    }

    private void shutDown() {
        mExecutor.shutDown();
    }

    private void allCountDown() {
        for (int i = 0; i < mBuilder.mTotalThreadSize; i++) {
            mCompletedCount.countDown();
        }
    }

    private List<?> getWork(int workId) {
        if (workId >= 0 && workId < mBuilder.mTotalThreadSize) {
            return mBuilder.mWorkQueue.subList(getSingleWorkStartIndex(workId), getSingleWorkStartIndex(workId) + getSingleWorkSize(workId));
        } else {
            throw new IllegalArgumentException();
        }
    }

    private int getSingleWorkSize(int workId) {
        if (workId >= 0 && workId < mBuilder.mTotalThreadSize) {
            if (workId < mBuilder.mTotalThreadSize - 1) {
                return getEverySingleWorkSize();
            } else {
                return mBuilder.mWorkQueue.size() - getEverySingleWorkSize() * (mBuilder.mTotalThreadSize - 1);
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    private int getSingleWorkStartIndex(int workId) {
        if (workId >= 0 && workId < mBuilder.mTotalThreadSize) {
            return getEverySingleWorkSize() * workId;
        } else {
            throw new IllegalArgumentException();
        }
    }

    private int getEverySingleWorkSize() {
        return mBuilder.mWorkQueue.size() / mBuilder.mTotalThreadSize;
    }

    public static class Builder<E> {
        private List<E> mWorkQueue;
        private int mTotalThreadSize;
        private IWork mCallback;
        private int mMode = MODE_ATOMIC_WORK;

        public Builder() {
            mWorkQueue = null;
            mTotalThreadSize = 10;
        }

        public Builder setWork(List<E> work) {
            if (work == null) {
                throw (new IllegalArgumentException("The work is null"));
            }

            mWorkQueue = work;
            return this;
        }

        public Builder setTotalThreadSize(int size) {
            if (size > 0 && size < MAX_THREAD_SIZE) {
                mTotalThreadSize = size;
            } else {
                throw new IllegalArgumentException("total thread size just can in 1 to MAX_THREAD_SIZE : 100");
            }

            return this;
        }

        public Builder setMode(int mode) {
            if (mode != MODE_ATOMIC_WORK && mode != MODE_LIST_WORK) {
                throw new IllegalArgumentException();
            } else {
                mMode = mode;
            }

            return this;
        }

        public Builder setCallback(IWork callback) {
            if (callback instanceof IAtomicWork || callback instanceof IListWork) {
                mCallback = callback;
            } else {
                throw new IllegalArgumentException();
            }

            return this;
        }

        public Worker build() {
            if (mWorkQueue != null) {
                return new Worker(this);
            } else {
                throw new IllegalArgumentException("the work have not been initialized normally");
            }
        }
    }

    interface IWork {
        public void onComplete();
    }

    interface IAtomicWork<E> extends IWork {
        public void doWork(E arg);
    }

    interface IListWork<E> extends IWork {
        public void doWork(List<E> workQueue, int size);
    }
}
