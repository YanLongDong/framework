package com.dyl.common.module.framework.threadpool;



import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 高性能对象池
 *
 * @author justdebugit@gmail.com
 *
 * @param <V> ultimately value store in holder
 * @param <T> value holder 
 */
class  FastPool<T extends IEntryHolder<V>,V>{
	 
	   private static final Logger LOGGER = LoggerFactory.getLogger(FastPool.class);
	   protected final AbstractQueuedLongSynchronizer synchronizer;
	   protected final CopyOnWriteArrayList<IEntryHolder<V>> sharedList;//entry pool 
	   protected final AtomicLong sequence;

	   private final ThreadLocal<ArrayList<WeakReference<IEntryHolder<V>>>> threadList;

	   public FastPool()
	   {
	      this.sharedList = new CopyOnWriteArrayList<IEntryHolder<V>>();
	      this.synchronizer = new Synchronizer();
	      this.sequence = new AtomicLong(1);//sequence并非代表池中数量，而是使用次数，只增不减
	      this.threadList = new ThreadLocal<ArrayList<WeakReference<IEntryHolder<V>>>>();
	   }
	   
	   public FastPool(List<T> list){
		   this();
		   if (list==null||list.size()<2) {
			   throw new IllegalArgumentException("list size must greater than 2");
		   }
		   for (T holder : list) {
			  add(holder);
		   }
		   
	   }
	   
    /**
     * 获取对象，
     * 如果竞争不大，直接从threadlocl获取，避免遍历list cas读的消耗；
     * 否则遍历list cas写，写成功说明抢占对象成功；否则挂起一段时间等待释放
     * @param timeout
     * @param timeUnit
     * @return
     * @throws InterruptedException
     */
	@SuppressWarnings("unchecked")
	public T borrow(long timeout, final TimeUnit timeUnit) throws InterruptedException
	   {
	      if (!synchronizer.hasQueuedThreads()) {
	         final ArrayList<WeakReference<IEntryHolder<V>>> list = threadList.get();
	         if (list == null) {
	            threadList.set(new ArrayList<WeakReference<IEntryHolder<V>>>(16));
	         }
	         else {
	            for (int i = list.size() - 1; i >= 0; i--) {
	               final IEntryHolder<V> holder = list.remove(i).get();
	               if (holder != null && holder.state().compareAndSet(IEntryHolder.STATE_NOT_IN_USE, IEntryHolder.STATE_IN_USE)) {
	                  return (T) holder;
	               }
	            }
	         }
	      }
	      // 否则扫描列表
	      timeout = timeUnit.toNanos(timeout);
	      final long startScan = System.nanoTime();
	      final long originTimeout = timeout;
	      do {
	         long startSeq;
	         do {
	            startSeq = sequence.get();
	            for (final IEntryHolder<V> holder :sharedList) {
	               if (holder.state().compareAndSet(IEntryHolder.STATE_NOT_IN_USE, IEntryHolder.STATE_IN_USE)) {
	                  return (T)holder;
	               }
	            }
	         } while (startSeq < sequence.get());//有还回就重试；可以不经历入队列park的过程提高并发能力
                 //seq只增不减，tryAcquireShared仅仅判断距离startSeq赋值state有无变化，即有没有对象还回发生
	         if (!synchronizer.tryAcquireSharedNanos(startSeq, timeout)) {
	            return null;
	         }
	         final long elapsed = (System.nanoTime() - startScan);
	         timeout = originTimeout - Math.max(elapsed, 100L);  
	      }
	      while (timeout > 1000L);  // 1000ns 

	      return null;
	   }

	   /**
	    * 将对象还回
	    *
	    */
	   public void requite(final T holder)
	   {
	      if (holder.state().compareAndSet(IEntryHolder.STATE_IN_USE, IEntryHolder.STATE_NOT_IN_USE)) {
	         final ArrayList<WeakReference<IEntryHolder<V>>> list = threadList.get();
	         if (list != null) {
	            list.add(new WeakReference<IEntryHolder<V>>(holder));
	         }
	         synchronizer.releaseShared(sequence.incrementAndGet());
	      }
	      else {
	         LOGGER.warn("pool execute an object leak", holder.toString());
	      }
	   }

	   /**
	    * 添加新的对象
	    *
	    * @param holder  object holder
	    */
	   public void add(final T holder)
	   {
	      sharedList.add(holder);
	      synchronizer.releaseShared(sequence.incrementAndGet());
	   }
	   
	   
	   public boolean tryShrink()
	   {   
		   int pendingCount =  getPendingQueue();
		   if (pendingCount>0 || sharedList.size() < 2||getCount(IEntryHolder.STATE_IN_USE)>=sharedList.size()-1) {
			   return false;
		   }
		   IEntryHolder<V> toBeRemoved = null;
		   for (final IEntryHolder<V> holder :sharedList) {
               if (holder.state().compareAndSet(IEntryHolder.STATE_NOT_IN_USE, IEntryHolder.STATE_REMOVED)) {
            	   toBeRemoved = holder;
            	   break;
               }
           }
		   if (toBeRemoved==null) {
			  return false;
		   }
	       final boolean removed = sharedList.remove(toBeRemoved);
	       //这一条语句是很有必要的，为的是避免pengding线程永不苏醒
	       synchronizer.releaseShared(sequence.incrementAndGet());
	       if (!removed ) {
	         LOGGER.warn("shrink attempt failed");
	       }
	       return removed;
	   }

	   
	   public boolean remove(T holder){
		   final boolean removed = sharedList.remove(holder);
	       synchronizer.releaseShared(sequence.incrementAndGet());
	       if (!removed ) {
	         LOGGER.warn("shrink attempt failed");
	       }
	       return removed;
	   }
	   
	   /**
	    * 替换损坏的对象
	    *
	    */
	   public void replaceAndrequit(final T holder,final V value)
	   {
	      if (!holder.state().compareAndSet(IEntryHolder.STATE_IN_USE, IEntryHolder.STATE_REMOVED)) {
	         LOGGER.error("Attempt to remove an object error: {}", holder.toString());
	         throw new IllegalStateException("pool execute an object leak");
	      }
	      assert value!=null;
	      holder.set(value);
	      if (holder.state().compareAndSet(IEntryHolder.STATE_REMOVED, IEntryHolder.STATE_NOT_IN_USE)) {
		       synchronizer.releaseShared(sequence.incrementAndGet());
		  }else {
			  LOGGER.warn("Attempt to remove an object error: {}", holder.toString());
		      throw new IllegalStateException("pool execute an object leak");
		}
	      
	   }


	   /**
	    * 获取当前快照
	    *
	    * @param state  STATE_NOT_IN_USE or STATE_IN_USE
	    * @return 
	    */
	   @SuppressWarnings("unchecked")
	public List<T> values(final int state)
	   {
	      final ArrayList<T> list = new ArrayList<T>(sharedList.size());
	      if (state == IEntryHolder.STATE_IN_USE || state == IEntryHolder.STATE_NOT_IN_USE) {
	         for (final IEntryHolder<V> reference : sharedList) {
	            if (reference.state().get() == state) {
	               list.add((T) reference);
	            }
	         }
	      }
	      return list;
	   }
	   
	  public List<T> values()
	   {
	      ArrayList<T> list = new ArrayList<T>(sharedList.size());
	      return list;
	   }


	   /**
	    * 获得等待线程数
	    *
	    * @return 
	    */
	   public int getPendingQueue()
	   {
	      return synchronizer.getQueueLength();
	   }

	   /**
	    * 得到指定状态的对象总数
	    *
	    * @param state 指定状态
	    * @return 指定状态总数
	    */
	   public int getCount(final int state)
	   {
	      int count = 0;
	      for (final IEntryHolder<V> reference : sharedList) {
	         if (reference.state().get() == state) {
	            count++;
	         }
	      }
	      return count;
	   }

	   
	   /**
	    * 池中数量
	    *
	    * @return 池中数量
	    */
	   public int size()
	   {
	      return sharedList.size();
	   }

	   public void dumpState()
	   {
	      for (IEntryHolder<V> holder : sharedList) {
	         LOGGER.info(holder.toString());
	      }
	   }
	   
	   public long getSeq(){
			return sequence.get();
		}
	   
	   public class Synchronizer extends AbstractQueuedLongSynchronizer{

		private static final long serialVersionUID = 6305735919591847529L;

		/**
		 * 简单公平锁的实现，当自己不是头结点的下一个结点时，也即自己未曾在队列排队
		 * 返回值大于等于0将代表获取锁成功，返回值小于0代表获取锁失败，需要进行AQS队列
		 * getState()-seq代表这段时间变动过，有还回或者添加操作发生；否则就表示没有变动过
		 * getState()-seq-1 为了在传播唤醒后续节点时，在只有1个资源可用的情况下，选择不唤醒后继节点,
		 * 防止同时唤醒增加线程间竞争
		 */
		@Override
		protected long tryAcquireShared(final long seq) {
			return hasQueuedPredecessors() ? -1L : getState()-seq-1; 
		}

		
		@Override
		protected boolean tryReleaseShared(final long ignored) {
			setState(sequence.get());
			return true;
		}
      

	 }
}

