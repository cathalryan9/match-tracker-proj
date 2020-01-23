DIR="../../../media/sf_eclipse-workspace/match-tracker-proj/dashboard-client/"
ls $DIR
rsync -r -v src/ $DIR"src"
rsync -v package.json $DIR 
rsync -v .gitignore $DIR 
