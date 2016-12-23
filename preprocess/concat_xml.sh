if [ -e wiki ]; then
	rm wiki
fi

find /Users/suganuma/github/search-engine/data/full/raw/extracted -name "wiki*" | xargs cat >> wiki