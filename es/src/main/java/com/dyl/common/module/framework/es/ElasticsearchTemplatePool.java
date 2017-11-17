package com.dyl.common.module.framework.es;

import com.dyl.common.module.framework.threadpool.ObjectFactory;
import com.dyl.common.module.framework.threadpool.Pool;
import com.dyl.common.module.framework.threadpool.ScalableFastPool;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.net.InetAddress;


public class ElasticsearchTemplatePool  implements InitializingBean, DisposableBean{
	/**Elasticsearch 集群名称**/
	private String clusterName;
	/**Elasticsearch 集群连接 host port   添加规则 " , "逗号分隔**/
	private String hostPorts;
	/**ElasticsearchTemplatePool 最小连接数**/
	private int minPoolSize;
	/**ElasticsearchTemplatePool 最大连接数**/
	private int maxPoolSize;

	private Pool<TransportClient> transportClientPool;

	public void destroy() throws Exception {
		transportClientPool.close();
	}

	public void afterPropertiesSet() throws Exception {
		init();
	}

	public void init() throws Exception {
		Pool<TransportClient> transportClientPool = new ScalableFastPool<TransportClient>(10,20,new ObjectFactory<TransportClient>() {
			public TransportClient makeObject() {
				TransportClient client = null;
				final Settings settings = Settings.builder().put("cluster.name", getClusterName()).build();
				try {
					String hostPorts = getHostPorts();
					System.out.println(hostPorts);
					String hostport[] = hostPorts.split(",");
					if (hostport.length==1) {
						String ipport[] = hostport[0].split(":");
						String ip = ipport[0].trim();
						int port = Integer.parseInt(ipport[1].trim());
						client = TransportClient.builder().settings(settings).
								addPlugin(DeleteByQueryPlugin.class).
								addPlugin(ReindexPlugin.class).build().
								addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(ip), port));
					}else if (hostport.length==2) {

						String ipport1[] = hostport[0].split(":");
						String ip1 = ipport1[0].trim();
						int port1 = Integer.parseInt(ipport1[1].trim());

						String ipport2[] = hostport[1].split(":");
						String ip2 = ipport2[0].trim();
						int port2 = Integer.parseInt(ipport2[1].trim());

						client = TransportClient.builder().settings(settings).addPlugin(DeleteByQueryPlugin.class).
								addPlugin(ReindexPlugin.class).build().
								addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(ip1), port1))
								.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(ip2), port2));


					}else if (hostport.length==3) {

						String ipport1[] = hostport[0].split(":");
						String ip1 = ipport1[0].trim();
						int port1 = Integer.parseInt(ipport1[1].trim());

						String ipport2[] = hostport[1].split(":");
						String ip2 = ipport2[0].trim();
						int port2 = Integer.parseInt(ipport2[1].trim());

						String ipport3[] = hostport[2].split(":");
						String ip3 = ipport3[0].trim();
						int port3 = Integer.parseInt(ipport3[1].trim());
						
						client = TransportClient.builder().settings(settings).addPlugin(DeleteByQueryPlugin.class).
								addPlugin(ReindexPlugin.class).build().
								addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(ip1), port1))
								.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(ip2), port2))
								.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(ip3), port3));
					}else if (hostport.length>=4) {
						throw new Exception("目前不支持超过4个节点 作为ES TransportClient连接");
					};
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				System.out.println("创建第一个Client成功"+client);
				return client;
			}
			public void destroyObject(TransportClient client) throws Exception {
				client.close();
			}
		});
		setTransportClientPool(transportClientPool);
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getHostPorts() {
		return hostPorts;
	}

	public void setHostPorts(String hostPorts) {
		this.hostPorts = hostPorts;
	}

	public Pool<TransportClient> getTransportClientPool() {
		return transportClientPool;
	}

	public void setTransportClientPool(Pool<TransportClient> transportClientPool) {
		this.transportClientPool = transportClientPool;
	}

	public int getMinPoolSize() {
		return minPoolSize;
	}

	public void setMinPoolSize(int minPoolSize) {
		this.minPoolSize = minPoolSize;
	}

	public int getMaxPoolSize() {
		return maxPoolSize;
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

}
