SELECT        DATEADD(ss, Acct_Session_Time * - 1, Event_Timestamp) AS Logon_Timestamp, Event_Timestamp AS Logoff_Timestamp, User_Name, Calling_Station_Id AS MAC, Framed_IP_Address, NAS_Identifier, 
                         Acct_Input_Octets / 131072 AS Mbytes_In, Acct_Output_Octets / 131072 AS Mbytes_Out, Acct_Session_Time / 60 AS Session_Time_min, AP_Mac
FROM            (SELECT        Acct_Session_Time, Event_Timestamp, User_Name, Calling_Station_Id, Framed_IP_Address, NAS_Identifier, Acct_Input_Octets, Acct_Output_Octets, Acct_Session_Time AS Expr1,
                                                        (SELECT        TOP (1) Called_Station_Id
                                                          FROM            dbo.accounting_data AS b
                                                          WHERE        (timestamp < a.timestamp) AND (User_Name = a.User_Name) AND (Packet_Type = 1)
                                                          ORDER BY timestamp DESC) AS AP_Mac
                          FROM            dbo.accounting_data AS a
                          WHERE        (Packet_Type = 4) AND (Acct_Status_Type = 2)) AS x