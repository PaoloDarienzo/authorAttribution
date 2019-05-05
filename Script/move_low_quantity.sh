#! /bin/bash -p

shopt -s nullglob   # glob patterns that match nothing expand to nothing
shopt -s dotglob    # glob patterns expand names that start with '.'

destdir='00003_LESS_THAN_TWO'

[[ -d $destdir ]] || mkdir -- "$destdir"

for dir in * ; do
    [[ -L $dir ]] && continue               # Skip symbolic links
    [[ -d $dir ]] || continue               # Skip non-directories
    [[ $dir -ef $destdir ]] && continue     # Skip the destination dir.

    numfiles=$(find "./$dir//." -type f -print | grep -c //)
    (( numfiles > 2 )) && mv -v -- "$dir" "$destdir"
done
