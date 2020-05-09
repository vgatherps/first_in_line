name=$1

cur_checksum=$(md5sum $name)

while true; do
	checksum=$(md5sum $name)
	if [ "$cur_checksum" != "$checksum" ]
	then
		echo "Uploading new file old $cur_checksum new $checksum";
		aws s3api put-object --acl public-read --bucket samtradesbitcoin.com --content-type "text/html" --key index.html --body "$name"
		cur_checksum="$checksum";
	fi
	sleep 1
done
