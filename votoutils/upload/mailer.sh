# Utility script to send message passed to callum
cmd=$(echo -e "$1" | NULLMAILER_NAME="Pipeline monitor" mail -s $2 $3)
