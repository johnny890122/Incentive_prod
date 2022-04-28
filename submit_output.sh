jupyter nbconvert .\Main_daily.ipynb --to python
python Main_daily.py
rm Main_daily.py
git status
git add -A
git commit -m 'dayily update'
