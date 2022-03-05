
sp_configure 'show advanced options', 1;
GO
RECONFIGURE;
GO
 
sp_configure 'Database Mail XPs', 1;
GO
RECONFIGURE
GO

-- Create a Database Mail profile  
EXECUTE msdb.dbo.sysmail_add_profile_sp  
    @profile_name = 'Notifications1',  
    @description = 'Profile used for sending outgoing notifications using Gmail.' ;  

GO

-- Grant access to the profile to the DBMailUsers role  
EXECUTE msdb.dbo.sysmail_add_principalprofile_sp  
    @profile_name = 'Notifications1',  
    @principal_name = 'public',  
    @is_default = 1 ;

	-- Create a Database Mail account  
EXECUTE msdb.dbo.sysmail_add_account_sp  
    @account_name = 'Gmail1',  
    @description = 'Mail account for sending outgoing notifications.',  
    @email_address = 'pkarunakar81@gmail.com',  
    @display_name = 'Automated Mailer',  
    @mailserver_name = 'smtp.gmail.com',--smtp.gmail.com
    @port = 587,--993
    @enable_ssl = 1,
    @username = 'pkarunakar81@gmail.com',
    @password = '76@Photosmy' ;  
GO

-- Add the account to the profile  
EXECUTE msdb.dbo.sysmail_add_profileaccount_sp  
    @profile_name = 'Notifications1',  
    @account_name = 'Gmail1',  
    @sequence_number =1 ;  
GO

If for some reason, execution of the code above returns an error, use the following code to roll back the changes:

EXECUTE msdb.dbo.sysmail_delete_profileaccount_sp @profile_name = 'Notifications1'
EXECUTE msdb.dbo.sysmail_delete_principalprofile_sp @profile_name = 'Notifications1'
EXECUTE msdb.dbo.sysmail_delete_account_sp @account_name = 'Gmail1'
EXECUTE msdb.dbo.sysmail_delete_profile_sp @profile_name = 'Notifications1'


EXEC msdb.dbo.sp_send_dbmail
     @profile_name = 'Notifications1',
     @recipients = 'pkarunakar81@gmail.com',--pkarunakar81@outlook.com'
     @body = 'The database mail configuration was completed successfully.',
     @subject = 'Automated Success Message',
	 @body_format = 'text';
GO



SELECT [sysmail_server].[account_id],
       [sysmail_account].[name] AS [Account Name],
       [servertype],
       [servername] AS [SMTP Server Address],
       [Port]
FROM [msdb].[dbo].[sysmail_server]
     INNER JOIN [msdb].[dbo].[sysmail_account] ON [sysmail_server].[account_id] = [sysmail_account].[account_id];

	 SELECT sent_account_id, sent_date FROM msdb.dbo.sysmail_sentitems;
	 SELECT * FROM msdb.dbo.sysmail_event_log;

	 SELECT * FROM msdb.dbo.ssysmail_servertype