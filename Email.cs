/* Email.cs
 * SSIS Script Loader by Karan Misra (kid0m4n)
 */

using System;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.Net.Mail;
using System.Net;
using System.IO;
using System.Text;
using System.Linq;
using System.Data.Linq;

namespace ST_ea8b62ccdbd845e7a8a1104cf064c859.csproj
{
    [System.AddIn.AddIn("ScriptMain", Version = "1.0", Publisher = "", Description = "")]
    public partial class ScriptMain : Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase
    {
        #region VSTA generated code
        enum ScriptResults
        {
            Success = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success,
            Failure = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
        };
        #endregion

        private string _emailFrom;
        private string _emailTo;
        
        private string _smtpServer;
        private int _smtpPort;
        private string _userName;
        private string _password;
        private string _domain;

        private string _metaTableConn;

        private string _parentPackageName;
        private string _parentTaskName;
        private DateTime _parentStartTime;
        private DateTime _parentEndTime;
        private string _parentVersion;
        private int _parentStatusID;

        private T GetVariable<T>(string varName)
        {
            return (T)Dts.Variables[varName].Value;
        }

        private void InitializeVariables()
        {
            _emailFrom = (string)Dts.Variables["v_EmailFrom"].Value;
            _emailTo = (string)Dts.Variables["v_EmailTo"].Value;
            _smtpServer = (string)Dts.Variables["v_EmailSmtpServer"].Value;
            _smtpPort = GetVariable<int>("v_EmailSmtpPort");
            _userName = (string)Dts.Variables["v_EmailUserName"].Value;
            _password = (string)Dts.Variables["v_EmailPassword"].Value;
            _domain = (string)Dts.Variables["v_EmailDomain"].Value;

            _metaTableConn = GetVariable<string>("v_MetaConn");

            _parentPackageName = (string)Dts.Variables["v_ParentPackageName"].Value;
            _parentTaskName = GetVariable<string>("v_ParentTaskName").ToUpper();
            _parentStartTime = (DateTime)Dts.Variables["v_ParentStartTime"].Value;
            _parentEndTime = DateTime.Now;
            _parentVersion = string.Format(
                "{0}.{1}.{2}",
                Dts.Variables["v_ParentVersionMajor"].Value,
                Dts.Variables["v_ParentVersionMinor"].Value,
                Dts.Variables["v_ParentVersionBuild"].Value);

            _parentStatusID = GetVariable<int>("v_ParentStatusID");
        }

        private void SendEmail(
            string from,
            string to,
            string subject,
            string body,
            bool isBodyHtml,
            string smtpServer,
            int smtpPort,
            string userName,
            string password,
            string domain,
            string[] attachments)
        {
            var smtpClient = new SmtpClient(smtpServer, smtpPort);
            var message = new MailMessage();

            message.From = new MailAddress(from);

            foreach (var toAddress in to.Split(';'))
                message.To.Add(toAddress);

            message.Subject = subject;
            message.Body = body;

            message.IsBodyHtml = isBodyHtml;

            if (string.IsNullOrEmpty(userName))
                smtpClient.UseDefaultCredentials = true;
            else
                smtpClient.Credentials = new NetworkCredential(userName, password, domain);

            if (attachments != null)
            {
                int i = 0;
                foreach (var attachment in attachments)
                {
                    if (attachment == null)
                        continue;

                    var stream = new MemoryStream(Encoding.ASCII.GetBytes(attachment));

                    message.Attachments.Add(new Attachment(stream, "ExecutionReport" + i++ + ".html"));
                }
            }

            smtpClient.Send(message);
        }

        public void Main()
        {
            InitializeVariables();

            string subject = null, bodyText = null;

            using (LoggingDataContext dc = new LoggingDataContext(_metaTableConn))
            {
                bool hasFailures, hasSuccesses;
                string failureAttachment = null, successAttachment = null;

                var errors = from e in dc.Failures
                             where e.Package_Name == _parentPackageName && e.Error_Time >= _parentStartTime
                             select e;

                if ((hasFailures = (errors.Count() > 0)))
                {
                    var sb = new StringBuilder();

                    sb.AppendLine("<html>");
                        sb.AppendLine("<head></head>");
                        sb.AppendLine("<body>");
                            sb.AppendLine("<h4 style=\"color: red;\">Package Name: " + _parentPackageName + "</h4>");
                            sb.AppendLine("<h4 style=\"color: red;\">Package Version: " + _parentVersion + "</h4>");
                            sb.AppendLine("<h4 style=\"color: red;\">Package Start Time: " + _parentStartTime.ToString() + "</h4");
                            sb.AppendLine("<h4 style=\"color: red;\">Package End Time: " + _parentEndTime.ToString() + "</h4>");
                            sb.AppendLine("<table border=\"2\">");
                                sb.AppendLine("<tr><th>Task Name</th><th>Version</th><th>Error Num</th><th>Error Desc</th><th>Error Time</th></tr>");

                                foreach (var e in errors)
                                {
                                    sb.AppendLine("<tr><td>" + e.Task_Name.ToNullString() + "</td><td>" + e.Version.ToNullString() + "</td><td>" + e.Error_Num.ToNullString() + "</td><td>" + e.Error_Desc.ToNullString() + "</td><td>" + e.Error_Time.ToNullString() + "</td></tr>");
                                }

                            sb.AppendLine("</table>");
                        sb.AppendLine("</body>");
                    sb.AppendLine("</html>");

                    failureAttachment = sb.ToString();                    
                }
                 
                var successes = from s in dc.Successes
                                where s.Package_Name == _parentPackageName && s.Starttime >= _parentStartTime
                                select s;

                if (hasSuccesses = (successes.Count() > 0))
                {
                    var sb = new StringBuilder();

                    sb.AppendLine("<html>");
                    sb.AppendLine("<head></head>");
                    sb.AppendLine("<body>");
                    sb.AppendLine("<h4 style=\"color: blue;\">Package Name: " + _parentPackageName + "</h4>");
                    sb.AppendLine("<h4 style=\"color: blue;\">Package Version: " + _parentVersion + "</h4>");
                    sb.AppendLine("<h4 style=\"color: blue;\">Package Start Time: " + _parentStartTime.ToString() + "</h4");
                    sb.AppendLine("<h4 style=\"color: blue;\">Package End Time: " + _parentEndTime.ToString() + "</h4>");
                    sb.AppendLine("<table border=\"2\">");
                    sb.AppendLine("<tr><th>Task Name</th><th>Imported Row Count</th><th>Inserted Row Count</th><th>Deleted Row Count</th><th>Updated Row Count</th><th>Start Time</th><th>End Time</th><th>Elapsed Time</th></tr>");

                    foreach (var s in successes)
                    {
                        sb.AppendLine("<tr><td>" + s.Task_Name.ToNullString() + "</td><td>" + s.Imported_RowCount.ToNullString() + "</td><td>" + s.Inserted_RowCount.ToNullString() + "</td><td>" + s.Del_Trunc_RowCount.ToNullString() + "</td><td>" + s.Updated_RowCount.ToNullString() + "</td><td>" + s.Starttime.ToNullString() + "</td><td>" + s.Endtime.ToNullString() + "</td><td>" + s.ElapsedTime.ToNullString() + "</td></tr>");
                    }

                    sb.AppendLine("</table>");
                    sb.AppendLine("</body>");
                    sb.AppendLine("</html>");

                    successAttachment = sb.ToString();
                }

                if (hasFailures)
                {
                    subject = _parentPackageName + " Executed with errors";
                    bodyText = _parentPackageName + " Executed with errors. Please see attachments for details.";
                }
                else if (hasSuccesses)
                {
                    subject = _parentPackageName + " Executed Succesfully";
                    bodyText = _parentPackageName + " Executed Succesfully. Please see attachment for details.";
                }
                else
                {
                    subject = _parentPackageName + " Executed with unknown status";
                    bodyText = _parentPackageName + " Executed with unknown status. Please investigate further.";
                }

                SendEmail(_emailFrom, _emailTo, subject, bodyText, false, _smtpServer, _smtpPort, _userName, _password, _domain,
                    hasFailures && hasSuccesses ? new string[] { successAttachment, failureAttachment } :
                    hasFailures ? new string[] { failureAttachment } :
                    hasSuccesses ? new string[] { successAttachment } : null);

                if (_parentStatusID != -1)
                {
                    var ps = dc.PackageStatus.Single(x => x.package_id == _parentStatusID);

                    ps.run_flag = hasFailures ? 'N' : 'Y';
                    ps.run_date = _parentEndTime;

                    dc.SubmitChanges();
                }
            }

            Dts.TaskResult = (int)ScriptResults.Success;
        }
    }
}