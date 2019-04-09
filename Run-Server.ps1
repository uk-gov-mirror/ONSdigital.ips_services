
$Env:DB_SERVER='localhost'
$Env:DB_USER_NAME='ips'
$Env:DB_PASSWORD='ips'
$Env:DB_NAME='ips'

waitress-serve --listen=*:5000 --threads=4 ips.app:app
