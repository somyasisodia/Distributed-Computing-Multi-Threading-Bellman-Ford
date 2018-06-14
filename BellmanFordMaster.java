import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import javax.swing.plaf.synth.SynthSpinnerUI;


public class BellmanFordMaster {

	static int adjMatrix[][];
	static HashMap<Node,HashSet<Node>> adjList=new HashMap<Node,HashSet<Node>>();
	static Map<Node, ConcurrentLinkedQueue<Message>> messageQueue = new HashMap<>();
	static HashMap<Integer,Node> NodeMap=new HashMap<>();

	static boolean start=false;
	static int round=0;
	static boolean complete=false;
	static int source;
	
	
	public BellmanFordMaster(int num){
		adjMatrix=new int[num][num];
	}
	
	public static class Message{
		
		int distanceFromStart;
		int pid;
		String type;
		public Message(int sender,int distanceFromStart,String type){
			this.pid=sender;
			this.distanceFromStart=distanceFromStart;
			this.type=type;
		}
		
	}
	public static class Node implements Callable<Boolean>{
		
		Node parent;
		int pid;
		int distanceFromStart;
		HashSet<Node> neighbors = new HashSet<Node>();
		int ack,rej;
		boolean ackSent;
		HashSet<Node> nodeChildren=new HashSet<Node>();
		HashMap<Node,ArrayList<Message>> messageList=new HashMap<Node,ArrayList<Message>>();
		HashSet<Node> response=new HashSet<Node>();
		public Node(int pid){
			this.pid=pid;
			parent=null;
			ack=0;
			rej=0;
			ackSent=false;
		}
		@Override
		public Boolean call() throws Exception {
			if(start==true){
				sendMessage();
			}
			else{
				receiveMessage();
			}
			return true;
		
		}
		
		public void receiveMessage() throws InterruptedException{
		
			
			while(!messageQueue.get(this).isEmpty()){	
				Message message=messageQueue.get(this).poll();
				if(message.type=="N"){
					
					if(distanceFromStart>message.distanceFromStart+adjMatrix[pid][message.pid]){
						distanceFromStart=message.distanceFromStart+adjMatrix[pid][message.pid];
						Node parentEx=this.parent;
						if(parentEx!=null){
							response.add(this.parent);
							Message msg=new Message(pid,0,"R");
							addNewMessage(parentEx,msg);
						}
						//System.out.println(this.pid+" "+message.pid);
						this.parent=NodeMap.get(message.pid); 
						//System.out.println("Current pid"+this.pid+" parent "+this.parent.pid);
						for(Node node:neighbors){
							
								System.out.println("From"+message.pid+" "+this.pid+" Prepping Normal message to "+node.pid);
								Message msg=new Message(pid,distanceFromStart,"N");
								addNewMessage(node,msg);
							
							
						}
					}else{
						
						System.out.println(this.pid+" Preparing to reject  "+message.pid);
						//rejects.add(NodeMap.get(message.pid));
						Message msg=new Message(pid,0,"R");
						addNewMessage(NodeMap.get(message.pid),msg);
						
					}
				}
				
				if(message.type=="R"){
					System.out.println("Rejected Candidate "+this.pid+" by "+message.pid);
					response.add(NodeMap.get(message.pid));
;					rej++;
				}
				if(message.type=="A"){
					System.out.println("Accepted Candidate "+pid+" by "+message.pid);
					response.add(NodeMap.get(message.pid));
					nodeChildren.add(NodeMap.get(message.pid));
					ack++;
				}
				
			}

			if(response.size()==neighbors.size()&&!ackSent){
				if(this.pid==source){
					complete=true;
					
				}else{
					System.out.println(this.pid+" Prepping acknowledgement to "+this.parent.pid);
				Message msg=new Message(this.pid,0,"A");
				addNewMessage(this.parent,msg);
				
				}
				ackSent=true;
				
			}
		//	System.out.println("PID "+this.pid+"ack "+ack);
		//	System.out.println("PID "+this.pid+" rej "+rej);
		
		}
		public void sendMessage(){
			
			for (Node node:messageList.keySet()){
				
				if(!messageList.get(node).isEmpty()){
					System.out.println("Message sent from  " + pid + " to " + node.pid);
					for(Message message:messageList.get(node)){
						messageQueue.get(node).add(message);
					}
                //messageQueue.get(node).addAll(messageQueue.get(node));
				}
			
            }
			messageList.clear(); // Clear messagelist 
			
			
		}
		public void initializeNodes(int source){
		
				if(pid==source){
					distanceFromStart=0;
					Message message=new Message(pid,distanceFromStart,"N");
					
					for (Node neighbor : neighbors){
						addNewMessage(neighbor, message);
	                }
				}
				else
					distanceFromStart=Integer.MAX_VALUE;
			}
		
		private void addNewMessage(Node neighbor, Message message) {
		
			ArrayList<Message> messages = messageList.get(neighbor);//, message);
            if (messages == null){
                messages = new ArrayList<Message>();
                messageList.put(neighbor, messages);
               
            }
            
            messages.add(message);
		}
			
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		Scanner sc=new Scanner(System.in);
		HashSet<Node> nodes=new HashSet<>();
		Queue<Integer> queue=new LinkedList<>();
		System.out.println("Enter the input file");
		String fileName=sc.nextLine();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
		String line=br.readLine();
		int num=Integer.parseInt(line);
		BellmanFordMaster b=new BellmanFordMaster(num);
		line=br.readLine();
		String split[]=line.split("\\s+");
		int counter=0;
		for(int i=0;i<split.length;i++){
			int pid=Integer.parseInt(split[counter++]);
			Node node=new Node(pid);
			nodes.add(node);
			NodeMap.put(pid, node);
			queue.add(pid);

		}
		
		line=br.readLine();
		source=Integer.parseInt(line);
		
		while((line=br.readLine())!=null){
			
			String splitLine[]=line.split("\\s+");
			int neighbour=0;  //neighbour for current node
			int pid=queue.poll();
				for(String s:splitLine){
					
					if(s.equals("-1")){
						neighbour++;
						continue;
					}
					for(Node node:nodes){
						
						if(node.pid==neighbour){
							NodeMap.get(pid).neighbors.add(node);
						}
					}
					
					adjMatrix[pid][neighbour]=Integer.parseInt(s);
					adjMatrix[neighbour][pid]=Integer.parseInt(s);
					neighbour++;
				}
				messageQueue.put(NodeMap.get(pid), new ConcurrentLinkedQueue<Message>());
				
			//	adjList.put(NodeMap.get(pid), node.neighbors);
			
			
		}
		
		/*for(Node n:nodes){
			System.out.println("");
			System.out.print(n.pid+"->");
			for(Node neighbour:n.neighbors){
				System.out.print(neighbour.pid+" ");
			}
		}*/
		
		for(Node node:nodes){
			node.initializeNodes(source);
		}
	
		b.start(nodes,num);
	}
	
	public void start(HashSet<Node> nodes,int num) throws InterruptedException, ExecutionException{

		while(!complete){
			start=true;
			ExecutorService execStart=Executors.newFixedThreadPool(1);
			List<Future<Boolean>> futuresStart=execStart.invokeAll((Collection<? extends Callable<Boolean>>) nodes);
			execStart.shutdown();
		
			for(Future<Boolean> future:futuresStart){
				future.get();
			}
			start=false;
			ExecutorService execEnd=Executors.newFixedThreadPool(1);
			List<Future<Boolean>> futuresEnd=execEnd.invokeAll((Collection<? extends Callable<Boolean>>) nodes);
			execEnd.shutdown();
		
			for(Future<Boolean> future:futuresEnd){
				future.get();
			
			}
			start=true;	round++;
		
			System.out.println("Round "+ round+" complete -> All messages delivered");
			System.out.println("-----------------------------------------------------");
			if(complete){
				break;
			}
		}// end of while
		System.out.println("-------------------");
		System.out.println("OUTPUT");
		System.out.println("-------------------");
		for(Node node:nodes){
			if(!node.nodeChildren.isEmpty()){
				for(Node n:node.nodeChildren){
					System.out.println(node.pid+"-> "+n.pid+" (distance = "+n.distanceFromStart+")");
				}
			}
		}
	}

}
