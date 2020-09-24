<html>
<head>
    <title>spring boot form submit example</title>
</head>
<body>
<h2>Employee Details</h2>
<form method="post" action="User">   // saveDetails url mapping in EmployeeController
    Enter Employee Name : <input type="text" name="userName"/>
    Enter Employee Address: <input type="text" name="uaddress">
    Enter Employee State: <input type="text" name="ustate">
    <input type="submit" value="Submit">
</form>
</body>
</html>