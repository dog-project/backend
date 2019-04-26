echo "Ranked Pairs: " >> rankings.txt
./xranking --credentials scripts/credentials.json --method ranked_pairs --output_format python_array >> rankings.txt
echo "\n\nCopeland: " >> rankings.txt
./xranking --credentials scripts/credentials.json --method copeland --output_format python_array >> rankings.txt 
echo "\n\nElo: " >> rankings.txt
./xranking --credentials scripts/credentials.json --method elo --output_format python_array >> rankings.txt
echo "\n\nMinimax: " >> rankings.txt
./xranking --credentials scripts/credentials.json --method minimax --output_format python_array >> rankings.txt
echo "\n\nWin Ratio: " >> rankings.txt
./xranking --credentials scripts/credentials.json --method win_ratio --output_format python_array >> rankings.txt
echo "\n\nWin/Tie Ratio:" >> rankings.txt
./xranking --credentials scripts/credentials.json --method win_tie_ratio --output_format python_array >> rankings.txt
echo "\n\n" >> rankings.txt

cat rankings.txt
