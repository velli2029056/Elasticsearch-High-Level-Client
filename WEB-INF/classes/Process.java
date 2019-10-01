import java.io.*;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.json.JSONArray;
import org.json.JSONObject;
import java.lang.*;
import java.util.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
public class Process
{
	public RestHighLevelClient connection()
	{
		RestHighLevelClient client = new RestHighLevelClient(
			        RestClient.builder(
			                new HttpHost("localhost", 9200, "http"),
			                new HttpHost("localhost", 9200, "http")));
		return client;
	}
	public boolean check_exist(String indexname,RestHighLevelClient client,PrintWriter out)
	{
		boolean exists=false;
		GetIndexRequest req = new GetIndexRequest(indexname);
		 try {
			exists = client.indices().exists(req, RequestOptions.DEFAULT);
			}
		catch(Exception e)
			{
				out.println("<p>"+e+"</p>");
			}
		return exists;
	}
	public static void insert(String sql,PrintWriter out)
	{
		 Process p=new Process();
		 RestHighLevelClient client =p.connection();
		 String arr[]=sql.split(" ");
		 int len=arr.length;
		 boolean exists=p.check_exist(arr[2],client,out);
		if(exists)
		{
			if(arr[0].equalsIgnoreCase("insert") && arr[1].equalsIgnoreCase("into") && arr[3].equalsIgnoreCase("values"))
			{
				BulkRequest request = new BulkRequest(); 
				String values[]=arr[4].split("\\(|\\)");
				ArrayList<String>[] listvalues = new ArrayList[values.length];
				for(int i=0;i<values.length/2;i++)
					listvalues[i]=new ArrayList<String>();
				for(int i=0,j=0;i<values.length;i++)
				{
					if(i%2!=0)
					{
						String vals[]=values[i].split(",");
						for(String s:vals)
							listvalues[j].add(s);
						j++;
					}
				}
				for(int i=0;i<values.length/2;i++)
				{
					if(listvalues[i].size()==10)
					{
						String json= "{" +
							"\"firstname\":\""+listvalues[i].get(0)+"\","+
							"\"lastname\":\""+listvalues[i].get(1)+"\","+
							"\"address\":\""+listvalues[i].get(2)+"\","+
							"\"id\":\""+listvalues[i].get(3)+"\","+
							"\"company\":\""+listvalues[i].get(4)+"\","+
							"\"location\":\""+listvalues[i].get(5)+"\","+
							"\"salary\":\""+listvalues[i].get(6)+"\","+
							"\"experience\":\""+listvalues[i].get(7)+"\","+
							"\"designation\":\""+listvalues[i].get(8)+"\","+
							"\"password\":\""+listvalues[i].get(9)+"\""+
							"}";
						request.add(new IndexRequest(arr[2]) 
								.source(json,XContentType.JSON));
						try {
						BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
							if(bulkResponse!=null)
								out.println("<p>Your data inserted successfully</p>");
						}
						catch(Exception e)
						{
							out.println("<p>"+e+"</p>");
						}
					}
					else
					{
						out.println("<p>Some column missing</p>");
					}
				}
			}
			else if(arr[0].equalsIgnoreCase("insert") && arr[1].equalsIgnoreCase("into"))
			{
				BulkRequest request = new BulkRequest(); 
				String values[]=arr[3].split("\\(|\\)");
				ArrayList<String>[] columns = new ArrayList[values.length];
				columns[0]=new ArrayList<String>();
				for(String s1:values)
				{
					String vals[]=s1.split(",");
					for(int j=0;j<vals.length;j++){
							if(!vals[j].equalsIgnoreCase(""))
							columns[0].add(vals[j]);
					}
				}
				String values1[]=arr[5].split("\\(|\\)");
				ArrayList<String>[] listvalues = new ArrayList[values1.length];
				for(int i=0;i<values1.length/2;i++)
					listvalues[i]=new ArrayList<String>();
				for(int i=0,j=0;i<values1.length;i++)
				{
					if(i%2!=0)
					{
						String vals[]=values1[i].split(",");
						for(String s:vals)
							listvalues[j].add(s);
						j++;
					}
				}
				for(int i=0;i<values1.length/2;i++)
				{
					if(listvalues[i].size()==columns[0].size())
					{
						String json="",json1="";
						for(int j=0;j<listvalues[i].size();j++)
						{
							if(j==0)
							json = "{" +
							"\""+columns[0].get(j)+"\":\""+listvalues[i].get(j)+"\","+"";
							else if(j!=listvalues[i].size()-1)
							{
							json="\""+columns[0].get(j)+"\":\""+listvalues[i].get(j)+"\","+"";
							}
							else
							{
								json="\""+columns[0].get(j)+"\":\""+listvalues[i].get(j)+"\""+
									"}";
							}
							json1+=json;
						}
						request.add(new IndexRequest(arr[2]) 
									.source(json1,XContentType.JSON));
							try {
							BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
								if(bulkResponse!=null)
									out.println("<p>Your data inserted successfully</p>");
							}
							catch(Exception e)
							{
								out.println("<p>"+e+"</p>");
							}
					}
					else
					{
						out.println("<p>Some column missing</p>");
					}
				}
			}
		}
		else
		{
			out.println("<p>Your specified index is not exist</p>");
		}
	}
	public static void update(String sql,PrintWriter out)
	{
		 Process p=new Process();
		 RestHighLevelClient client =p.connection();
		 String arr[]=sql.split(" ");
		 int len=arr.length;
		 boolean exists=p.check_exist(arr[1],client,out);
		if(exists)
		{
			if(arr[0].equalsIgnoreCase("update") && arr[2].equalsIgnoreCase("set") && arr[4].equalsIgnoreCase("where"))
			{
				char op = 0;
				for (char ch : arr[5].toCharArray()) 
				{
				    if (!Character.isDigit(ch) && !Character.isLetter(ch))
				    {
				    	op=ch;
				    }
				}
				String values[]=arr[3].split(",");
				Map<String,String> m=new HashMap<String,String>();
				for(int i=0;i<values.length;i++)
				{
					String vals[]=values[i].split("=");
					m.put(vals[0],vals[1]);
				}
				if(op=='=')
				{
					BulkRequest request = new BulkRequest(); 
					String condition[]=arr[5].split("=");
					for(Entry<String, String> entry:m.entrySet())
					{
						String json="{"+
								"\""+entry.getKey()+"\":\""+entry.getValue()+"\"" +
								"}";
						request.add(new UpdateRequest(arr[1],condition[1]) 
								.doc(json,XContentType.JSON));
					}
					try
					{
						BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
						if(bulkResponse!=null)
								out.println("<p>Your details should be updated successfully</p>");
					}
					catch(Exception e)
					{			
							out.println("<p>"+e+"</p>");
					}
				}
				else if(op=='<')
				{
				try {
						UpdateByQueryRequest requested = new UpdateByQueryRequest(arr[1]); 
						requested.setConflicts("proceed"); 
						requested.setMaxDocs(10000); 
						for(Entry<String, String> entry:m.entrySet())
						{
						System.out.println(entry.getKey());
						String column=entry.getKey();
						String value=entry.getValue();
						requested.setScript(new Script(ScriptType.INLINE,
							        "painless","if(ctx._source."+arr[5]+"){ctx._source."+column+"="+value+";}" ,Collections.<String, Object>emptyMap())); 
						}
						BulkByScrollResponse bulkResponse =client.updateByQuery(requested, RequestOptions.DEFAULT);
						long updateddocuments = bulkResponse.getUpdated();
						if(bulkResponse!=null)
						out.println("<p>Your details should be updated successfully,No of documents updated:"+updateddocuments+"</p>");
					}
					catch(Exception e)
					{
						out.println("<p>"+e+"</p>");
					}
				}
				else if(op=='>')
				{
				try {
						UpdateByQueryRequest requested = new UpdateByQueryRequest(arr[1]); 
						requested.setConflicts("proceed"); 
						requested.setMaxDocs(10000); 
						for(Entry<String, String> entry:m.entrySet())
						{
						String column=entry.getKey();
						String value=entry.getValue();
						requested.setScript(new Script(ScriptType.INLINE,
							        "painless","if(ctx._source."+arr[5]+"){ctx._source."+column+"="+value+";}" ,Collections.<String, Object>emptyMap())); 
						}
						BulkByScrollResponse bulkResponse =client.updateByQuery(requested, RequestOptions.DEFAULT);
						long updateddocuments = bulkResponse.getUpdated();
						if(bulkResponse!=null)
						out.println("<p>Your details should be updated successfully,No of documents updated:"+updateddocuments+"</p>");
					}
					catch(Exception e)
					{
						out.println("<p>"+e+"</p>");
					}
				}
			}
		}
		else
		{
			out.println("<p>Your specified index is not exist</p>");
		}
	}
	public static void create(String sql,PrintWriter out)
	{
		 Process p=new Process();
		 RestHighLevelClient client =p.connection();
		 String arr[]=sql.split(" ");
		 int len=arr.length;
		 boolean exists=p.check_exist(arr[2],client,out);
		if(!exists)
		{
			if(len>3 && arr[0].equalsIgnoreCase("create") && arr[1].equalsIgnoreCase("table"))
			{
				CreateIndexRequest request = new CreateIndexRequest(arr[2]);
				request.settings(Settings.builder() 
					    .put("index.number_of_shards", 3)
					    .put("index.number_of_replicas", 2)
					); 
				String pro[]=arr[4].split(",");
				Map<String, Object> properties = new HashMap<String, Object>();
				for(int i=0;i<pro.length;i++)
				{
					String field[]=pro[i].split("=");
					Map<String, Object> fields = new HashMap<String, Object>();
					fields.put("type",field[1]);
					properties.put(field[0], fields);	
				}
				Map<String, Object> mapping = new HashMap<String, Object>();
				mapping.put("properties", properties);
				request.mapping(mapping); 
				try {
					CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);		
					if(createIndexResponse!=null)
					{
						out.println("<p>Successfully creating the index</p>");
					}
				}
				catch(Exception e) {
					out.println("<p>"+e+"<p>");
				}
			}
			else if(len==3 && arr[0].equalsIgnoreCase("create") && arr[1].equalsIgnoreCase("table"))
			{
				CreateIndexRequest request = new CreateIndexRequest(arr[2]);
				request.settings(Settings.builder() 
					    .put("index.number_of_shards", 3)
					    .put("index.number_of_replicas", 2)
					); 
				try {
					CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);		
					if(createIndexResponse!=null)
					{
						out.println("<p>Successfully creating the index</p>");
					}
				}
				catch(Exception e) {
					out.println("<p>"+e+"</p>");
				}
			}
			else
			{
				out.println("<p>Sql Query error</p>");
			}
		}
		else
		{
			out.println("<p>Specified index already exist</p>");
		}
	}
	public static void delete(String query,PrintWriter out)
	{
		Process p=new Process();
		RestHighLevelClient client =p.connection();
		String arr[]=query.split(" ");
		int len=arr.length;
		if(arr[0].equalsIgnoreCase("delete") && arr[1].equalsIgnoreCase("from"))
		{
			boolean exists=p.check_exist(arr[2],client,out);
			if(exists)
			{
				if(arr[3].equalsIgnoreCase("where"))
				{
					Map<String,String> m=new HashMap<String,String>();
					char op = 0;
					for (char ch : arr[4].toCharArray()) 
					{
					    if (!Character.isDigit(ch) && !Character.isLetter(ch))
					    {
					    	op=ch;
					    }
					}
					if(op=='=')
						{
							String temp[]=arr[4].split("=");
							m.put(temp[0],temp[1]);
						try {
								DeleteByQueryRequest requested = new DeleteByQueryRequest(arr[2]); 
								requested.setConflicts("proceed"); 
								requested.setMaxDocs(10000); 
								for (Entry<String, String> entry : m.entrySet())  
								{
									QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(entry.getKey(),entry.getValue());
									requested.setQuery(matchQueryBuilder);
								}
								BulkByScrollResponse bulkResponse =client.deleteByQuery(requested, RequestOptions.DEFAULT);
								long deleteddocuments = bulkResponse.getDeleted();
								if(bulkResponse!=null)
								out.println("<p>Your details should be deleted successfully ,No of document deleted is:"+deleteddocuments+"</p>");
							}
							catch(Exception e)
							{
								out.println("<p>"+e+"</p>");
							}
						}
						else if(op=='<')
						{
							String temp[]=arr[4].split("<");
							m.put(temp[0],temp[1]);
						try {
								DeleteByQueryRequest requested = new DeleteByQueryRequest(arr[2]); 
								requested.setConflicts("proceed"); 
								requested.setMaxDocs(10000); 
								for (Entry<String, String> entry : m.entrySet())  
								{
									QueryBuilder matchQueryBuilder = QueryBuilders.rangeQuery(entry.getKey()).lt(entry.getValue());
									requested.setQuery(matchQueryBuilder);
								}
								BulkByScrollResponse bulkResponse =client.deleteByQuery(requested, RequestOptions.DEFAULT);
								long deleteddocuments = bulkResponse.getDeleted();
								if(bulkResponse!=null)
								out.println("<p>Your details should be deleted successfully ,No of document deleted is:"+deleteddocuments+"</p>");
							}
							catch(Exception e)
							{
								out.println("<p>"+e+"</p>");
							}
						}
						else if(op=='>')
						{
							String temp[]=arr[4].split(">");
							m.put(temp[0],temp[1]);
						try {
								DeleteByQueryRequest requested = new DeleteByQueryRequest(arr[2]); 
								requested.setConflicts("proceed"); 
								requested.setMaxDocs(10000); 
								for (Entry<String, String> entry : m.entrySet())  
								{
									QueryBuilder matchQueryBuilder = QueryBuilders.rangeQuery(entry.getKey()).gt(entry.getValue());
									requested.setQuery(matchQueryBuilder);
								}
								BulkByScrollResponse bulkResponse =client.deleteByQuery(requested, RequestOptions.DEFAULT);
								long deleteddocuments = bulkResponse.getDeleted();
								if(bulkResponse!=null)
									out.println("<p>Your details should be deleted successfully ,No of document deleted is:"+deleteddocuments+"</p>");
							}
							catch(Exception e)
							{
								out.println("<p>"+e+"</p>");
							}
						}
				}
				else
				{
						try
						{	
							DeleteByQueryRequest requests =new DeleteByQueryRequest(arr[2]);
							QueryBuilder matchQueryBuilder = QueryBuilders.matchAllQuery();
							requests.setQuery(matchQueryBuilder); 
							BulkByScrollResponse bulkResponse =client.deleteByQuery(requests, RequestOptions.DEFAULT);
							long deleteddocuments = bulkResponse.getDeleted();
							if(bulkResponse!=null)
							out.println("<p>Your details should be deleted successfully="+deleteddocuments+"</p>");
						}
						catch(Exception e)
						{
							out.println("<p>"+e+"</p>");
						}
				}
			}
			else
			out.println("<p>"+arr[2]+" index is not exist in the elastic search cluster</p>");
		}
		else if(arr[0].equalsIgnoreCase("drop") && arr[1].equalsIgnoreCase("table") && len>=2)
		{
			boolean exists=p.check_exist(arr[2],client,out);
			if(exists)
			{
				try{
				DeleteIndexRequest request = new DeleteIndexRequest(arr[2]);
				AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
				if(deleteIndexResponse!=null)
				  out.println("<p>Your index is deleted successfully<p>");
				}
				catch(Exception e)
				{
					out.println("<p>"+e+"<p>");
				}
			}
			else
			{
				out.println("<p>Your specified index is not present in that cluster</p>");
			}
		}
	}
	public static void retrieve(String sql,PrintWriter out)
	{
		 Process p=new Process();
		 RestHighLevelClient client =p.connection();
		 String arr[]=sql.split(" ");
		 boolean exists=p.check_exist(arr[3],client,out);
		 if(exists)
		 {
			try 
			{
				RestClient restClient = RestClient.builder(
					new HttpHost("localhost", 9200, "http"),
					new HttpHost("localhost", 9201, "http")).build();
				Request request = new Request( "POST", "/_sql");
				request.addParameter("format", "json");
				request.setJsonEntity("{\"query\":\""+sql+"\"}");
				Response response = restClient.performRequest(request);
				String str= EntityUtils.toString(response.getEntity());
				out.println("<html>");
				out.println("</style></head>");
				out.println("<body><fieldset><table class='table'><thead><tr>");
				JSONObject json = new JSONObject(str);
				JSONArray jsonArray = (JSONArray)json.get("columns");
				for (int i = 0; i <jsonArray.length(); i++) {
				   JSONObject obj= (JSONObject) jsonArray.get(i);
				   String columnname=(String) obj.get("name");
				   String type=(String) obj.get("type");
				   out.println("<th scope='col'>"+columnname+"</th>");        
				}
				out.println("</tr></thead>");
				JSONArray jsonArray1 = (JSONArray)json.get("rows");
				  for (int i = 1; i <jsonArray1.length(); i++) 
				  {
					  out.println("<tr>");
					  JSONArray obj= (JSONArray) jsonArray1.get(i);
					  String str1=obj.toString();
					  String rows[]=str1.split("\\[|\\,|\\\"|\\]");
					  int k=0;
					  for(String t:rows)
					  {
						  if(!t.equals(""))
							out.println("<td id='col1'>"+t+"</td>");
					  }
					  out.println("</tr>");
				  }
				  out.println("</table></fieldset>");
				  out.println("</body></html>");
			}
			catch(Exception e)
			{
				out.println("<p>"+e+"</p>");
			}
		 }
		 else
		 {
			 out.println("<p>Your specified index is not exists</p>");
		 }
	}
}