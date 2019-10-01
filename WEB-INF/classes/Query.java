import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;
public class Query extends HttpServlet
{
	public void doGet(HttpServletRequest request,HttpServletResponse response)throws ServletException,IOException
	{
			response.setContentType("text/html");
			PrintWriter out=response.getWriter();
			String sql=request.getParameter("id");
			out.println("<html>");
			String sp[]=sql.split(" ");
			if(sp[0].equalsIgnoreCase("insert"))
			{
					Process.insert(sql,out);
			}
			if(sp[0].equalsIgnoreCase("update"))
			{
					Process.update(sql,out);
			}
			if(sp[0].equalsIgnoreCase("create"))
			{
					Process.create(sql,out);
			}
			if(sp[0].equalsIgnoreCase("delete")||sp[0].equalsIgnoreCase("drop"))
			{
					Process.delete(sql,out);
			}
			if(sp[0].equalsIgnoreCase("select"))
			{
					Process.retrieve(sql,out);
			}
	}
}