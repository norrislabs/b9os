pathadd() {
    newelement=${1%/}
    if [ -d "$1" ] && ! echo "$PATH" | grep -E -q "(^|:)$newelement($|:)" ; then
        PATH="$newelement:$PATH"
    fi
}

pypathadd() {
    newelement=${1%/}
    if [ -d "$1" ] && ! echo "$PYTHONPATH" | grep -E -q "(^|:)$newelement($|:)" ; then
        PYTHONPATH="$newelement:$PYTHONPATH"
    fi
}

master=${1-localhost}
masterIP=$(python -c "import socket; print(socket.gethostbyname(\"${master}\"))")
export B9_MASTER=$masterIP

if [ -d "$(pwd)"/b9os/b9cli ]
then
	# In parent of b9os directory
	pathadd "$(pwd)"/b9os/b9cli
	pypathadd "$(pwd)"/b9os
else
	if [ -d "$(pwd)"/b9cli ]
	then
		# In the b9os directory
		pathadd "$(pwd)"/b9cli
		pypathadd "$(pwd)"
	else
		# Who knows where the fuck we are
		echo "Need to be in the b9os directory or its parent."
		return
	fi
fi

export PATH
export PYTHONPATH

echo "Using master $master at $masterIP"
echo $PATH
echo $PYTHONPATH

cd b9ws || exit
