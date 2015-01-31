FROM haskell
MAINTAINER Michael Burge <michaelburge@pobox.com>

RUN cabal update
ADD ./yxdb-utils.cabal /opt/yxdb-utils/yxdb-utils.cabal
RUN cd /opt/yxdb-utils && cabal install --only-dependencies -j4

# All dependencies should be built and cached by this point, so we can change
# application code without needing to rebuild from scratch.
ADD . /opt/yxdb-utils/
RUN cd /opt/yxdb-utils && cabal install

ENV PATH /root/.cabal/bin:$PATH

WORKDIR /opt/yxdb-utils

CMD ["yxdb2csv"]
