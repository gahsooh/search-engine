if [ -e /Users/suganuma/github/search-engine/data/arrange/pages ]; then
	rm /Users/suganuma/github/search-engine/data/arrange/pages
fi

find /Users/suganuma/github/search-engine/data/full/raw/extracted -name "wiki*" | xargs cat >> /Users/suganuma/github/search-engine/data/arrange/pages