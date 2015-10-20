#!/bin/bash -e
PROJECT="qcat"
ORG_PATH="github.com/ghetzel"
REPO_PATH="${ORG_PATH}/${PROJECT}"

export GOPATH=${PWD}/gopath
export PATH="$GOPATH/bin:$PATH"

rm -rf $GOPATH/src/${REPO_PATH}
mkdir -p $GOPATH/src/${ORG_PATH}
ln -s ${PWD} $GOPATH/src/${REPO_PATH}


rm -rf $GOPATH/pkg


eval $(go env)

if [ -s DEPENDENCIES ]; then
  echo 'Processing dependencies...'

  for d in $(cat DEPENDENCIES); do
    go get $d
  done
fi

# set flags
[ "$DEBUG" == 'true' ] || GOFLAGS="-ldflags '-s'"


# build it!
echo "Building $REPO_PATH..."
go build -a $GOFLAGS -o bin/${PROJECT} ${REPO_PATH}/


# vendor the dependencies
echo 'Vendoring...'
# remove all .git directories except the local projects (that would be bad :)
find gopath -type d | grep -v "$REPO_PATH" | grep -v ^\./\.git$ | grep \.git$ | xargs rm -rf

echo 'DONE'
exit 0