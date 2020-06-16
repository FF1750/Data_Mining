import pandas as pd
import requests
import urllib
import numpy as np

import os.path
from os import path

from bs4 import BeautifulSoup

from datetime import datetime
from datetime import timedelta

import time
import random

from tqdm import tqdm

import sys 


from os import listdir
from os.path import isfile, join


#General functions

#Function to produce unique match IDs
def numerise_string(x):

	alphabet = "abcdefghijklmnopqrstuvwxyz"
	tag = ""

	for letters in x:
		tag = tag + str(alphabet.find(letters.lower()))

	return tag


#Custom append function used for auto-save purposes
def update_frame(data, new_data):

	if len(data) == 0:
		return new_data
	else:
		return data.append(new_data)




def string_to_array(x):

		dummy = "abcdefghijklmnopqrstuvwxyz"
		alphabet = []
		for letters in dummy:
			alphabet.append(letters)	
		alphabet = np.array(alphabet)		

		arr = []

		for letters in x.lower():
			arr.append(np.where(alphabet == letters)[0][0])	

		return np.array(arr)


def match_two_names(x, y):

	x_arr = string_to_array(x)
	y_arr = string_to_array(y)

	n = len(x_arr)
	m = len(y_arr)
	n_try = m - n + 1

	if n_try > 0:
		match_p = []

		for i in range(0, n_try):
			v = x_arr - y_arr[0:n]
			match_p.append(len(np.where(v == 0)[0]) / n)
			y_arr = y_arr[1:]





class Baseball_Scrapper:

	def __init__(self, file_path):

		self.file_path = file_path
		dir_path = file_path + "/MLB_Modeling"

		#Create the repositories if they do not exist
		#Main repo
		target = file_path + "/MLB_Modeling"
		if not path.exists(target):
			os.mkdir(target)
			print("Main directory created at:" + "\t" + target)

		#Sub-repo
		sub_directories = ["Bat", "Pitch", "Scores", "Betting", "Misc"]
		for element in sub_directories:

			target = dir_path + "/" + element
			if not path.exists(target):
				os.mkdir(target)
				print("Sub-directory created at:" + "\t" + target)


		#Sub-repo locations
		self.paths = []
		for element in sub_directories:
			self.paths.append(dir_path + "/" + element)


		dictio_path = self.paths[4] + "/Abreviations_Dictionary.csv"
		if path.exists(dictio_path):
			self.dictio = pd.read_csv(dictio_path)
		else:
			self.dictio = []


		print("Scapper succesfully initiated.")


	#Updates a file, or save it if it doesn't exists
	def update_file(self, save_path, file_name, data):

		final_path = save_path + "/" + file_name

		try:
			if not path.exists(final_path):
				if len(data) > 0:
					data.to_csv(final_path, index = False)

			else:
				if len(data) > 0:
					pd.read_csv(final_path).append(data).drop_duplicates().to_csv(final_path, index = False)

		except:
			print("Failed to update file.")	



	#Translates a team name to its city name
	def Translate_Team_Names(self, value, want):

		if len(self.dictio) == 0:
			sys.exit("Missing file:" + "\t" + self.paths[4] + "Abreviations_Dictionary.csv")
		else:
			x = self.dictio

		m = len(x.columns)


		for j in range(0, m):
			location = np.where(x.iloc[:, j] == value)[0]

			if len(location) > 0:
				return x.at[location[0], want]


	#Frame-wide translate
	def Fix_Team_Names(self, frame, want):

		for col in frame.columns:

			if "Team" in col or "Opponent" in col:

				vector = np.array(list(frame[col]))
				values = np.array(list(set(vector)))

				m = np.where(frame.columns == col)[0][0]

				for team in values:

					index = np.where(vector == team)[0]
					proper_name = self.Translate_Team_Names(team, want)
					
					frame.iloc[index, m] = proper_name

		return frame




	###########################################################
	###########################################################
	######################## WORK FLOW ########################
	###########################################################
	###########################################################


	###########################################################
	#################### WEB SCRAPPING ########################
	###########################################################


	#Attempts to extract game URLs from a certain date
	#Is used inside a loop
	def Scrape_FanGraphs_game_url(self, date):

		url = "https://www.fangraphs.com/scoreboard.aspx?date=" + date

		html = requests.get(url).content
		html_content = BeautifulSoup(html, 'lxml')

		links = html_content.findAll('a')
		game_url = []

		for link in links:
			try:
				href = link.attrs['href']
			except:
				continue

			if "boxscore" in href:
				game_url.append("https://www.fangraphs.com/" + href)

		return game_url


	#Scrapes FanGraphs.com for urls to games that were played between to dates (frm, to)
	#Is used to initiate the database
	#Once done, the UPDATE_FanGraphs_Box_Scores method should be used
	def Get_FanGraphs_Game_URLs(self, frm, to):

		begin = datetime.strptime(frm, "%Y-%m-%d")
		end = datetime.strptime(to, "%Y-%m-%d")

		n = (end - begin).days + 1

		urls = pd.DataFrame(columns = ["URL"])
		no_games_dates = pd.DataFrame(columns = ["Dates"])
		games_dates = pd.DataFrame(columns = ["Dates"])


		#Check for dates which links were already scrapped
		if path.exists(self.paths[-1] + "/Game_Dates.csv"):
			dates_done = list(pd.read_csv(self.paths[-1] + "/Game_Dates.csv")["Dates"])
		else:
			dates_done = []

		#Main loop (extraction + auto-save)
		for i in tqdm(range(0, n)):

			date = datetime.strftime(begin, "%Y-%m-%d")

			#Avoid extracting for certain cases
			if (begin.month < 3) or (begin.month > 10) or (date in dates_done):
				begin = begin + timedelta(days = 1)
				continue

			#Retrieve links
			try:
				todays_url = self.Scrape_FanGraphs_game_url(date)
			except:
				no_games_dates = no_games_dates.append(pd.DataFrame(date, columns = ["Dates"]))
				begin = begin + timedelta(days = 1)
				continue

			if len(todays_url) > 0:
				urls = urls.append(pd.DataFrame(todays_url, columns = ["URL"]))
				games_dates = games_dates.append(pd.DataFrame([date], columns = ["Dates"])) 

				print("Scrapped:" + "\t" + date)


			#Saving procedure (trigerred every 20 iterations)
			if i % 20 == 0 or begin == end:

				self.update_file(self.paths[-1], "Game_URLs.csv", urls)
				urls = pd.DataFrame(columns = ["URL"])

				self.update_file(self.paths[-1], "No_Game_Dates.csv", no_games_dates)
				no_games_dates = pd.DataFrame(columns = ["Dates"])

				self.update_file(self.paths[-1], "Game_Dates.csv", games_dates)
				games_dates = pd.DataFrame(columns = ["Dates"])			

				print("Saved data.")


			begin = begin + timedelta(days = 1)
			time.sleep(random.randint(5, 10))

		print("Done.")



	#Get the Box Scores data based off a URL 
	def Scrape_FanGraphs_game_stats_by_url(self, ulr):

		html = requests.get(url).content
		tables = pd.read_html(html)

		#Date and team names
		url_split = url.split("-")
		date = url_split[0].split("=")[-1] + "-" + url_split[1] + "-" + url_split[2].split("&")[0]

		date_index = -1
		for table in tables:
			date_index += 1
			if table.iloc[0,0] == "Team":
				break	

		home_team = tables[date_index].iloc[2, 0]
		away_team = tables[date_index].iloc[1, 0]	

		#Score
		home_score = tables[date_index].iloc[2, -1]
		away_score = tables[date_index].iloc[1, -1]

		ID = ""
		temp = date.split("-")
		for values in temp:
			ID = ID + values

		ID = ID + numerise_string(home_team[0:2] + away_team[0:2])

		scores = pd.DataFrame(columns = ["Home", "Home_Score", "Away", "Away_Score", "Date", "URL", "ID"])
		scores.loc[0] = [home_team, home_score, away_team, away_score, date, url, ID]


		#Find where the extraction should begin
		start = 0
		for table in tables:
			start += 1
			if str(type(table.columns)) == "<class 'pandas.core.indexes.multi.MultiIndex'>":
				break

		tables = tables[start:]

		#Find the play by play table
		table_lengths = []
		for table in tables:
			table_lengths.append(len(table))

		table_lengths = np.array(table_lengths)

		play_by_play_index = np.where(table_lengths == np.max(table_lengths))[0][0]
		play_by_play = tables[play_by_play_index]
		del tables[play_by_play_index]
		table_lengths = np.delete(table_lengths, play_by_play_index)

		#Merge the frames
		merged_tables = []
		for i in range(0, 4):

			temp_table = tables[i]
			for j in range(4, len(tables)):

				size = len(temp_table)

				if len(tables[j]) == size:

					check = len(np.where(tables[i]["Name"] == tables[j]["Name"])[0])
					if check == size:

						temp_table = pd.merge(temp_table, tables[j], on = "Name")

			temp_table["Date"] = date
			if i % 2 == 0:
				temp_table["Team"] = home_team
				temp_table["Location"] = "Home"
				temp_table["Opponent"] = away_team
			else:
				temp_table["Team"] = away_team
				temp_table["Location"] = "Away"
				temp_table["Opponent"] = home_team

			colnames = []
			for j in range(0, len(temp_table.columns)):
				colnames.append(temp_table.columns[j].split("_")[0])

			temp_table.columns = colnames
			temp_table["ID"] = ID

			merged_tables.append(temp_table.loc[:,~temp_table.columns.duplicated()])




		merged_tables.append(scores)

		return merged_tables



	#Extracts the box scores based off the URL list
	def Extract_FanGraphs_Box_Scores(self):

		url_path = self.paths[-1] + "/Game_URLs.csv"
		if path.exists(url_path):

			urls = list(set(list(pd.read_csv(url_path)["URL"])))


			#Checks for existing Box_Scores
			path_to_check = self.paths[2] + "/FanGraphs_Scores.csv"
			if path.exists(path_to_check):
				urls_done = list(pd.read_csv(path_to_check).drop_duplicates()["URL"])

				urls = [x for x in urls if x not in urls_done]


			#Initialise variables

			bat = []
			pitch = []
			scores = []

			count = 0
			n = len(urls)

			print("Extracting " + str(n) + " Box Scores...")
			#e_time = round((((45/2) + 3) * n) / 60, 2)
			#print("Estimated running time:" + "\t" + str(e_time) + " minutes")

			#Loop throught URLs 
			for i in tqdm(range(0, n)):

				url = str(urls[i])
				count += 1
				try:
					tables = self.Scrape_FanGraphs_game_stats_by_url(url)
				except:
					time.sleep(random.randint(5,10))
					continue

				bat = update_frame(bat, tables[0].append(tables[1]))
				pitch = update_frame(pitch, tables[2].append(tables[3]))
				scores = update_frame(scores, tables[4])

				print("\t" + "\t" + "\t" + "***** ADDED GAME *****")
				print(scores.iloc[-1,:])

				#print(scores)

				if count % 20 == 0 or url == urls[-1]:

					self.update_file(self.paths[0], "FanGraphs_Box_Scores.csv", bat)	
					bat = []

					self.update_file(self.paths[1], "FanGraphs_Box_Scores.csv", pitch)	
					pitch = []					

					self.update_file(self.paths[2], "FanGraphs_Scores.csv", scores)	
					scores = []

					print("\t" + "\t" + "\t" + "***** PROGRESS SAVED *****")

				if url != urls[-1]:
					time.sleep(random.randint(3, 7))




	###########################################################
	#################### UPDATE CODES  ########################
	###########################################################


	#MAIN FUNCTION
	#Scrapes within the interval [last_scrapped, today]
	#Update the Box_Scores
	#Clean the data if needed
	def UPDATE_FanGraphs_Box_Scores(self):

		path_check = self.paths[2] + "/FanGraphs_Scores.csv"
		if not path.exists(path_check):
			sys.exit("Missing file:" + "\t" + path_check)

		frm = pd.read_csv(path_check)["Date"].sort_values("Date",ascending=False)["Date"]
		n = len(frm)
		frm = frm[0]
		to = datetime.strftime(to, "%Y-%m-%d")

		self.Get_FanGraphs_Game_URLs(frm, to)
		self.Extract_FanGraphs_Box_Scores()

		n_new = len(pd.read_csv(path_check))
		if n_new > n:
			self.Clean_Data()
		else:
			print("No new Box Scores to scrape.")



	#Extracts the box scores based off the URL list
	def Extract_FanGraphs_Box_Scores_FROM_MISSING_MATCHES(self):

		url_path = self.paths[-1] + "/Missing_Matches.csv"
		if path.exists(url_path):

			file_missing_urls = pd.read_csv(url_path)

			urls = list(set(list(file_missing_urls["URL"])))

			#Checks for existing Box_Scores
			path_to_check = self.paths[2] + "/FanGraphs_Scores.csv"
			if path.exists(path_to_check):
				urls_done = list(pd.read_csv(path_to_check).drop_duplicates()["URL"])

				urls = [x for x in urls if x not in urls_done]


			#Initialise variables

			bat = []
			pitch = []
			scores = []

			count = 0
			n = len(urls)

			print("Extracting " + str(n) + " Box Scores...")
			#e_time = round((((45/2) + 3) * n) / 60, 2)
			#print("Estimated running time:" + "\t" + str(e_time) + " minutes")

			#Loop throught URLs 
			for i in tqdm(range(0, n)):

				url = str(urls[i])
				count += 1
				try:
					tables = self.Scrape_FanGraphs_game_stats_by_url(url)
				except:
					time.sleep(random.randint(5,10))
					continue

				bat = update_frame(bat, tables[0].append(tables[1]))
				pitch = update_frame(pitch, tables[2].append(tables[3]))
				scores = update_frame(scores, tables[4])

				print("\t" + "\t" + "\t" + "***** ADDED GAME *****")
				print(scores.iloc[-1,:])

				#print(scores)

				if count % 20 == 0 or url == urls[-1]:

					self.update_file(self.paths[0], "FanGraphs_Box_Scores.csv", bat)	
					bat = []

					self.update_file(self.paths[1], "FanGraphs_Box_Scores.csv", pitch)	
					pitch = []					

					self.update_file(self.paths[2], "FanGraphs_Scores.csv", scores)	
					scores = []

					print("\t" + "\t" + "\t" + "***** PROGRESS SAVED *****")

				if url != urls[-1]:
					time.sleep(random.randint(3, 7))


	###########################################################
	#################### DATA CLEANING  #######################
	###########################################################

	#Cleans the bat, pitch and scores frames
	def Clean_Data(self):

		#Create sub-repositories if they do not already exist
		sufix = "/Clean_Data"

		for i in range(0, (len(self.paths) - 1)):
			path_string = self.paths[i] + sufix
			if not path.exists(path_string):
				os.mkdir(path_string)
				print("Create sub-directory at:" + "\t" + path_string)

		scores_path = self.paths[2] + "/FanGraphs_Scores.csv"
		if not path.exists(scores_path):
			sys.exit("No data to clean.")
		else:
			scores = pd.read_csv(scores_path)

		scores.columns = ["Team_Home", "Score_Home", "Team_Away", "Score_Away", "Date", "URL", "ID"]

		#Load bat and pitch frames
		frames = []
		for i in range(0,2):

			path_string = self.paths[i] + "/FanGraphs_Box_Scores.csv"
			if not path.exists(path_string):
				sys.exit("Missing file:" + "\t" + path_string)
			else:
				frames.append(pd.read_csv(path_string, dtype={'a': str})) 


		#Use CITY abreviations for TEAMS
		scores = self.Fix_Team_Names(scores, "City")
		for i in range(0,2):
			frames[i] = self.Fix_Team_Names(frames[i], "City")


		#Find double-matches days
		IDs = np.array(list(scores["ID"]))
		doubles = [x for x in IDs if len(np.where(IDs == x)[0]) > 1]

		if len(doubles) > 0:

			fix = list(set(list(doubles)))

			m = np.where(scores.columns == "ID")[0][0]

			for values in fix:

				index_scores = np.where(IDs == values)[0][1]

				for i in range(0, 2):

					index = np.where(frames[i]["ID"] == values)[0]
					temp_names = frames[i].iloc[index, :]["Name"]

					split = np.where(temp_names == "Total")[0][1] + 1
					to_replace = index[split:]

					col_index = np.where(frames[i].columns == "ID")[0][0]

					frames[i].iloc[to_replace, col_index] = -values


				scores.iloc[index_scores, m] = -values


		for i in range(0, 2):

			x = frames[i]

			#Remove "Total" rows
			rmv = np.where(x["Name"] == "Total")[0]
			x = x.drop(rmv).reset_index(drop = True)


			#The are NaNs due to ratios
			#Create dummy columns for NaNs

			n_NaNs = x.isna().sum()
			fix = np.where(n_NaNs > 0)[0]
			cols_to_fix = x.columns[fix]

			if len(fix) > 0:
				for cnames in cols_to_fix:

					#Replace with 0
					col_index = np.where(x.columns == cnames)[0][0]
					to_replace = np.where(x[cnames].isna())[0]

					if "%" in cnames or cnames == "HR/FB":
						x.iloc[to_replace, col_index] = "0.0%"
					else:
						if x[cnames].dtype == np.float64:
							x.iloc[to_replace, col_index] = 0.0
						else:
							x.iloc[to_replace, col_index] = 0

					#Add dummy column
					new_name = cnames + "_NaN"
					x[new_name] = 0

					col_index = np.where(x.columns == new_name)[0][0]
					x.iloc[to_replace, col_index] = 1


			#Format percentages
			data_types = list(x.dtypes)
			for j in range(0, len(x.columns)):
				if data_types[j] == np.float64 or data_types[j] == np.int64:
					continue
				
				else:
					m = x.columns[j]

					if ("%" in m and not "NaN" in m) or m == "HR/FB":
						try:
							x[m] = x[m].str.replace("%", "").astype(float) / 100
						except:
							problem = [k for k, x in enumerate(list(x[m])) if "," in x]	
							index_col = np.where(x.columns == m)[0][0]
							x.iloc[problem, index_col] = "0.0%"

							x[m] = x[m].str.replace("%", "").astype(float) / 100

					else:
						try:
							x[m] = x[m].astype(float)
						except:
							continue


			#Fix column_names
			colnames = list(x.columns)
			for j in range(0, len(colnames)):

				if colnames[j][0] == "-":
					colnames[j] = colnames[j][1:] + "_minus"
				elif x.columns[j][0] == "+":
					colnames[j] = colnames[j][1:] + "_plus"		

			x.columns = colnames		


			#Add position variable
			#Only for bat
			try:

				splitted_names = pd.DataFrame(list(x["Name"].str.split(" - ")), columns = ["Name", "Position"])

				x["Name"] = (splitted_names["Name"] + " " +  x["Team"]).str.replace(" ", "")

				temp = list(set(list(splitted_names["Position"])))

				positions = list()
				for values in temp:

					y = values.split("-")
					for vals in y:
						if not vals in positions:
							positions.append(vals)

				position_names = []

				for values in positions:
					c_name = "Position_" + values
					x[c_name] = 0
					position_names.append(c_name)


				for j in range(0, len(x)):

					y = splitted_names["Position"][j].split("-")
					for values in y:
						c_name = "Position_" + values
						x.at[j, c_name] = 1

				frames[i] = x.sort_values("Date", ascending=False)

			except:

				splitted_names = pd.DataFrame(list(x["Name"].str.split("(")), columns = ["Name", "Position"])
				x["Name"] = (splitted_names["Name"] + " " +  x["Team"]).str.replace(" ", "")
				frames[i] = x.sort_values("Date", ascending=False)


		scores = scores.sort_values("Date", ascending = False)
		for i in range(0, 2):
			frames[i] = frames[i].sort_values("Date", ascending = False)

		#Save the cleaned files
		for i in range(0, 2):
			save_path = self.paths[i] + "/Clean_Data/FanGraphs_Box_Scores.csv"
			frames[i].to_csv(save_path, index = False)
			print("Saved:" + "\t" + save_path)

		save_path = self.paths[2] + "/Clean_Data/FanGraphs_Scores.csv"
		scores = scores.sort_values("Date", ascending=False)
		scores.to_csv(save_path, index = False)
		print("Saved:" + "\t" + save_path)


		print("Cleaning done.")


	#Cleans betting data
	def Clean_Betting_Data(self):

		#Set sub-directory up
		path_data = self.paths[3] + "/Clean_Data"
		if not path.exists(path_data):
			os.mkdir(path_data)
			print("Created sub-directory at:" + "\t" + path_data)

		#Extract CSV files if needed
		url = "https://www.sportsbookreviewsonline.com/scoresoddsarchives/mlb/mlboddsarchives.htm"
		html = requests.get(url).content
		html_content = BeautifulSoup(html, 'lxml')
		links = html_content.findAll('a')

		file_url = []
		for link in links:
			try:
				href = link.attrs['href']
			except:
				continue

			if ".xlsx" in href:
				file_url.append(str("https://www.sportsbookreviewsonline.com/scoresoddsarchives/mlb/" + href))


		for x in file_url:

			year = x.split("%")[-1].split(".")[0][2:]
			path_save = self.paths[3] + "/MLB_Odds_" + str(year) + ".csv"
			if not path.exists(path_save):

				file = pd.read_excel(x)
				file.to_csv(path_save, index = False)
				print("Downloaded:" + "\t" + path_save)



		#Used to translate names to FanGraphs equivalents
		path_pitch = self.paths[1] + "/Clean_Data/FanGraphs_Box_Scores.csv"
		if not path.exists(path_pitch):
			sys.exit("Missing file:" + "\t" + path_pitch)

		FG_names = list(set(list(pd.read_csv(path_pitch)["Name"])))	
		FG_teams = []
		for names in FG_names:
			FG_teams.append(names[-3:])

		FG_teams = np.array(FG_teams)
		FG_names = np.array(FG_names)
		all_teams = np.array(list(set(list(FG_teams))))
		team_index = []
		for teams in all_teams:
			team_index.append(np.where(FG_teams == teams)[0])


		#Format the files
		frame = []

		for i in tqdm(range(2010, (datetime.now().year + 1))):

			path_check = self.paths[3] + "/MLB_Odds_" + str(i) + ".csv"
			if path.exists(path_check):

				temp = pd.read_csv(path_check).reset_index(drop = True)

				#Fix dates
				temp["Date"] = temp["Date"].astype(str)

				for j in range(0, len(temp)):
					u = temp.at[j, "Date"]
					if len(temp.at[j, "Date"]) == 3:
						temp.at[j, "Date"] = str(i) + "-" + "0" + u[0] + "-" + u[1:]
					else:
						temp.at[j, "Date"] = str(i) + "-" + u[0:2] + "-" + u[2:]


				#Convert moneyline values to returns
				moneylines = ["Open", "Close"]

				rmv = np.where(temp["Open"] == "NL")[0]
				if len(rmv) > 0:
					temp = temp.drop(rmv)
					temp = temp.reset_index(drop = True)
					temp["Open"] = temp["Open"].astype(int)
					temp["Close"] = temp["Close"].astype(int)

				for vals in moneylines:

					m = np.where(temp.columns == vals)[0][0]

					index = np.where(temp[vals] > 0)[0]
					temp.iloc[index, m] = temp.iloc[index, m] / 100

					index = np.where(temp[vals] < 0)[0]
					temp.iloc[index, m] = 100 / (- temp.iloc[index, m])									


				split_frames = []
				values = ["H", "V"]
				temp = temp[["Date", "Team", "Open", "Close", "VH", "Final", "Pitcher"]]

				temp["Pitcher"] = temp["Pitcher"].str.replace("-L", "").str.replace("-R", "")
				for j in range(0, len(temp)):
					temp.at[j, "Pitcher"] = str(temp.at[j, "Pitcher"])[1:]

				#Translate team names
				temp = self.Fix_Team_Names(temp, "City")
				temp = temp.reset_index(drop = True)


				#Translate names to their FanGraphs equivalents
				for j in range(0, len(temp)):
					index = np.where(all_teams == temp["Team"][j])[0][0]
					index = team_index[index]

					to_check_through = FG_names[index]
					k = 0
					for names in to_check_through:
						if temp["Pitcher"][j].lower() in names.lower():
							temp.at[j, "Pitcher"] = names


				for vals in values:

					index = np.where(temp["VH"] == vals)[0]
					split_frames.append(temp.iloc[index, :])
					del split_frames[-1]["VH"]

					if vals == "H":
						split_frames[-1].columns = ["Date", "Team_Home", "Open_Home", "Close_Home", "Score_Home", "Pitcher_Home"]
					else:
						split_frames[-1].columns = ["Date", "Team_Away", "Open_Away", "Close_Away", "Score_Away", "Pitcher_Away"]
						del split_frames[-1]["Date"]

					split_frames[-1] = split_frames[-1].reset_index(drop = True)

				#Assemble
				temp = pd.concat(split_frames, axis = 1)

				#Compute implied odds
				temp["Open_Winning_Odds_Home"] = 1 / (1 + temp["Open_Home"])
				temp["Close_Winning_Odds_Home"] = 1 / (1 + temp["Close_Home"])

				temp["Open_Winning_Odds_Away"] = 1 / (1 + temp["Open_Away"])
				temp["Close_Winning_Odds_Away"] = 1 / (1 + temp["Close_Away"])

				temp["Open_Winning_Odds_Home"] = temp["Open_Winning_Odds_Home"] / (temp["Open_Winning_Odds_Home"] + temp["Open_Winning_Odds_Away"])
				temp["Close_Winning_Odds_Home"] = temp["Close_Winning_Odds_Home"] / (temp["Close_Winning_Odds_Home"] + temp["Close_Winning_Odds_Away"])

				temp["Open_Winning_Odds_Away"] = 1 - temp["Open_Winning_Odds_Home"]
				temp["Close_Winning_Odds_Away"] = 1 - temp["Close_Winning_Odds_Home"]


				if len(frame) == 0:
					frame = temp
				else:
					frame = frame.append(temp)


		frame = frame.iloc[::-1]
		frame = frame.reset_index(drop = True)

		rmv = [k for k, x in enumerate(frame["Pitcher_Home"]) if x not in FG_names]
		rmv = rmv + [k for k, x in enumerate(frame["Pitcher_Away"]) if x not in FG_names]
		rmv = list(set(list(rmv)))

		if len(rmv) > 0:
			frame = frame.drop(rmv).reset_index(drop = True)


		#Attempt to add IDs
		path_scores = self.paths[2] + "/Clean_Data/FanGraphs_Scores.csv"
		if path.exists(path_scores):

			print("\t" + "\t" + "\t" + "***** Adding IDs *****")

			frame["ID"] = 0
			scores = pd.read_csv(path_scores)

			for i in tqdm(range(0, len(scores))):

				a = np.where(frame["Date"] == scores.at[i, "Date"])[0]
				if len(a) == 0:
					continue

				b = np.where(frame["Team_Home"][a] == scores.at[i, "Team_Home"])[0]
				if len(b) == 0:
					continue

				a = a[b]

				b = np.where(frame["Score_Home"][a] == scores.at[i, "Score_Home"])[0]
				if len(b) == 0:
					continue

				a = a[b]

				b = np.where(frame["Score_Away"][a] == scores.at[i, "Score_Away"])[0]
				if len(b) == 0:
					continue

				index = a[b]

				if len(index) > 0:
					frame.at[index[0], "ID"] = scores.at[i, "ID"]


		rmv = np.where(frame["ID"] == 0)[0]
		if len(rmv) > 0:
			frame = frame.drop(rmv)


		frame.to_csv(self.paths[3] + "/Clean_Data/MLB_Odds.csv", index = False)
		print("\t" + "\t" + "***** MLB Moneyline data successfully formated *****")



	########################################################################
	#################### INDIVIDUAL PLAYER DATABASE  #######################
	########################################################################

	#Give each player their own file
	def Build_Individual_Players_Database(self):

		for i in range(0,2):

			frame_path = self.paths[i] + "/Clean_Data/FanGraphs_Box_Scores.csv"
			if not path.exists(frame_path):
				sys.exit("Missing file:" + "\t" + frame_path)

			else:
				frame = pd.read_csv(frame_path)

				names = list(set(list(frame["Name"])))

				#Create sub-directory for by_player database
				sub_dir = self.paths[i] + "/Clean_Data/By_Player"
				if not path.exists(sub_dir):
					os.mkdir(sub_dir)
					print("Created sub-directory at:" + "\t" + sub_dir) 

				x = np.array(frame["Name"])



				rmv = []
				for name in tqdm(names):

					index = np.where(x == name)[0]
					temp_path = sub_dir + "/" + name + ".csv"
					frame.iloc[index, :].to_csv(temp_path, index = False)

					rmv = rmv + list(index)

					if len(rmv) > 25000:

						frame = frame.drop(rmv)
						frame = frame.reset_index(drop = True)
						x = np.delete(x, rmv)
						rmv = []

			print("Per-Player Database built at:" + "\t" + sub_dir)



	#Compute players' individual average stats for the previous n-games
	def Build_Individual_Players_ROLLING_AVERAGE_Database(self, n):

		for i in range(0,2):
			
			#Build directories
			sufix = "/Clean_Data/By_Player/Rolling_Averages"
			path_check = self.paths[i] + sufix
			if not path.exists(path_check):
				os.mkdir(path_check)
				print("Created sub-directory at:" + "\t" + path_check)

			sufix = sufix + "/" + str(n)
			path_check = self.paths[i] + sufix
			if not path.exists(path_check):
				os.mkdir(path_check)
				print("Created sub-directory at:" + "\t" + path_check)		


			#Build the database
			path_folder = self.paths[i] + sufix
			path_players = self.paths[i] + "/Clean_Data/FanGraphs_Box_Scores.csv"

			if not path.exists(path_players):
				sys.exit("Missing file:" + "\t" + path_players)

			players = list(set(list(pd.read_csv(path_players)["Name"])))

			for player in tqdm(players):
				path_player = self.paths[i] + "/Clean_Data/By_Player/" + player + ".csv"
				if not path.exists(path_player):
					continue

				data = pd.read_csv(path_player)

				data_numeric = data.select_dtypes(exclude = "O")
				data_string = data.select_dtypes(include = "O")
				data_ID = list(data["ID"])[:-1]

				new_cols = []

				n_iter = len(data) - 1
				for j in range(0, n_iter):

					frm = j + 1
					to = np.min([j + n, len(data) - 1])

					new_cols.append(list(data_numeric.loc[frm:to, :].copy().mean()))

				data_string = data_string.drop(len(data_string) - 1).reset_index(drop = True)
				data_numeric = pd.DataFrame(data = new_cols, columns = data_numeric.columns)

				data = pd.concat([data_string, data_numeric], axis = 1)
				data["ID"] = data_ID

				path_save = path_check +"/" + player + ".csv"
				data.to_csv(path_save, index = False)



	##############################################################
	#################### REGRESSION FRAME  #######################
	##############################################################


	def Prepare_Regression_Frames(self, n):


		total_frames = []
		for i in range(0,2):
			sufix = "/Clean_Data/By_Player/Rolling_Averages/" + str(n)
			path_check = self.paths[i] + sufix
			if not path.exists(path_check):
				sys.exit("Missing directory:" + "\t" + path_check)	

			all_frames = []

			files = [f for f in listdir(path_check) if isfile(join(path_check, f))]
			print("\t" + "\t" + "\t" + "*** Assembling player frames... ***")
			for vals in tqdm(files):
				all_frames.append(pd.read_csv(path_check + "/" + vals))

			frame = pd.concat(all_frames).reset_index(drop = True)

			save_path = path_check + "/All"
			if not path.exists(save_path):
				os.mkdir(save_path)
				print("Created sub-directory at:" + "\t" + save_path)

			save_path = save_path + "/All_Players.csv"

			#Find matches with missing players and purge them
			reference_frame = pd.read_csv(self.paths[i] + "/Clean_Data/FanGraphs_Box_Scores.csv")
			IDs = list(set(list(reference_frame["ID"])))

			frame_copy = frame.copy()

			rmv = []
			flush_frame = []
			flush_reference = []

			if i == 0:
				print("\t" + "\t" + "\t" + "*** Purging Matches with missing player data... ***")
				for ID in tqdm(IDs):
					index_frame = np.where(frame["ID"] == ID)[0]
					index_reference = np.where(reference_frame["ID"] == ID)[0]

					if len(index_frame) != len(index_reference):
						rmv = rmv + list(index_frame)

					flush_frame = flush_frame + list(index_frame)
					flush_reference = flush_reference + list(index_reference)

					if len(flush_reference) > 50000:
						frame = frame.drop(flush_frame).reset_index(drop = True)
						reference_frame = reference_frame.drop(flush_reference).reset_index(drop = True)

						flush_frame = []
						flush_reference = []

				if len(rmv) > 0:
					frame_copy = frame_copy.drop(rmv).reset_index(drop = True)

			total_frames.append(frame_copy)

		for i in range(0, 2):
			total_frames[i] = total_frames[i].sort_values(["Date", "ID", "Team"], ascending = False).reset_index(drop = True)

		print("\t" + "\t" + "\t" + "*** Merging betting and scores data... ***")
		#Obtain scores
		path_file = self.paths[2] + "/Clean_Data/FanGraphs_Scores.csv"
		if not path.exists(path_file):
			sys.exit("Missing file:" + "\t" + path_file)

		scores = pd.read_csv(path_file)
		scores["Win"] = 1
		losses = np.where(scores["Score_Home"] < scores["Score_Away"])[0]
		scores.iloc[losses, np.where(scores.columns == "Win")[0][0]] = 0

		#Obtain money line stats
		path_file = self.paths[3] + "/Clean_Data/MLB_Odds.csv"
		if not path.exists(path_file):
			sys.exit("Missing file:" + "\t" + path_file)

		money_lines = pd.read_csv(path_file)

		#Fuse both frames
		frame = pd.merge(scores, money_lines, on = "ID").reset_index(drop = True)
		rmv = [x for x in frame.columns if "_y" in x]
		frame = frame.drop(rmv, axis = 1)

		colnames = []
		for names in frame.columns:
			if "_x" in names:
				colnames.append(names.replace("_x", ""))
			else:
				colnames.append(names)

		frame.columns = colnames

		save_path = self.paths[3] + "/Clean_Data/Complete_Summary.csv"
		frame.to_csv(save_path, index = False)

		#Only keep starting pitchers
		print("\t" + "\t" + "\t" + "*** Tagging starting pitchers... ***")
		rows = []
		rmv = []
		teams = list(set(list(total_frames[1]["Team"])))
		teams_index = []
		for team in teams:
			teams_index.append(np.where(total_frames[1]["Team"] == team)[0])

		teams = np.array(teams)
		teams_index = np.array(teams_index)
		new_IDs = []
		new_location = []

		for i in range(0,2):
			total_frames[i] = total_frames[i].reset_index(drop = True)

		for i in tqdm(range(0, len(frame))):
			k = 0
			all_index = []
			home_index = teams_index[np.where(teams == frame.at[i, "Team_Home"])[0][0]]
			away_index = teams_index[np.where(teams == frame.at[i, "Team_Away"])[0][0]]
			all_index = np.array(list(home_index) + list(away_index))

			index = np.where(total_frames[1]["ID"][all_index] == frame.at[i, "ID"])[0]
			index = all_index[index]

			try:
				min_loc = np.min(index)
				home = np.where(total_frames[1]["Name"][home_index] == frame.at[i, "Pitcher_Home"])[0]
				home = home_index[home]
				home = home[np.where(home >= min_loc)[0][0]]
			except:
				k += 1

			try:
				away = np.where(total_frames[1]["Name"][away_index] == frame.at[i, "Pitcher_Away"])[0]
				away = away_index[away]
				away = away[np.where(away >= min_loc)[0][0]]	
			except:
				k += 1


			if k == 0:
				rows.append(home)	
				rows.append(away)
				for k in range(0,2):
					new_IDs.append(frame.at[i, "ID"])

				new_location.append("Home")
				new_location.append("Away")

			else:
				rmv.append(frame.at[i, "ID"])	

		total_frames[1] = total_frames[1].iloc[rows, :].reset_index(drop = True)
		total_frames[1]["ID"] = new_IDs
		total_frames[1]["Location"] = new_location


		#Keep common IDs
		IDs = list(frame["ID"])
		a = list(set(list(total_frames[0]["ID"])))
		b = list(set(list(total_frames[1]["ID"])))
		IDs = [x for x in IDs if x in a]
		IDs = [x for x in IDs if x in b]
		IDs = np.array(IDs)

		keep = [k for k, x in enumerate(np.array(frame["ID"])) if x in IDs]
		frame = frame.iloc[keep,:].reset_index(drop = True)


		print("\t" + "\t" + "\t" + "*** Building regression frames... ***")
		total_frames_numeric = []
		for i in range(0, 2):
			total_frames_numeric.append(total_frames[i].select_dtypes(exclude = "O"))
			if "ID" in total_frames_numeric[i].columns:
				del total_frames_numeric[i]["ID"]


		index_teams = []
		for i in range(0,2):
			dummy = []
			for team in teams:
				dummy.append(np.where(total_frames[i]["Team"] == team)[0])
			index_teams.append(dummy)


		rows = []
		IDs_final = []
		n_cols = 2 * (np.shape(total_frames_numeric[0])[1] + np.shape(total_frames_numeric[1])[1])
		for i in tqdm(range(0, len(frame))):

			try:

				index_home = np.where(teams == frame["Team_Home"][i])[0][0]
				index_home = index_teams[0][index_home]

				index_away = np.where(teams == frame["Team_Away"][i])[0][0]
				index_away = index_teams[0][index_away]

				index = list(index_home) + list(index_away)
				index = np.array(index)
				index = index[np.where(total_frames[0]["ID"][index] == frame.at[i, "ID"])[0]]

				location = total_frames[0]["Location"][index]
				temp = total_frames_numeric[0].iloc[index, ]
				m = np.where(temp.columns == "AB")[0][0]

				home = np.where(location == "Home")[0]
				away = np.where(location == "Away")[0]

				weights_home = temp.iloc[home, m]
				weights_home = weights_home / weights_home.sum()

				weights_away = temp.iloc[away, m]
				weights_away = weights_away / weights_away.sum()

				x_bat_home = pd.DataFrame(weights_home.transpose().dot(temp.iloc[home, :])).transpose()
				x_bat_away = pd.DataFrame(weights_away.transpose().dot(temp.iloc[away, :])).transpose()

				index = np.where(total_frames[1]["ID"] == frame.at[i, "ID"])[0]
				location = total_frames[1]["Location"][index]
				temp = total_frames_numeric[1].iloc[index, :]

				x_pitch_home = temp.iloc[np.where(location == "Home")[0], :]
				x_pitch_away = temp.iloc[np.where(location == "Away")[0], :]		

				cnames = []
				for values in x_bat_home.columns:
					cnames.append(values + "_Bat_Home")
				for values in x_pitch_home.columns:
					cnames.append(values + "_Pitch_Home")					
				for values in x_bat_home.columns:
					cnames.append(values + "_Bat_Away")
				for values in x_pitch_home.columns:
					cnames.append(values + "_Pitch_Away")	

				out = pd.concat([x_bat_home.reset_index(drop = True), x_pitch_home.reset_index(drop = True), x_bat_away.reset_index(drop = True), x_pitch_away.reset_index(drop = True)], axis = 1)
				out.columns = cnames

				if np.shape(out)[1] == n_cols:
					rows.append(out)
					IDs_final.append(frame.at[i, "ID"])

			except:
				continue


		print("\t" + "\t" + "\t" + "*** Saving... ***")
		regression_frame = pd.concat(rows, axis = 0)

		path_check = self.file_path + "/MLB_Modeling/Regression" 
		if not path.exists(path_check):
			os.mkdir(path_check)
			print("Created directory at:" + "\t" + path_check)

		path_check = path_check + "/" + str(n)
		if not path.exists(path_check):
			os.mkdir(path_check)
			print("Created sub-directory at:" + "\t" + path_check)

		regression_frame.to_csv(path_check + "/X.csv", index = False)
		frame.to_csv(path_check + "/Y.csv", index = False)

		print("\t" + "\t" + "\t" + "*** X and Y regression frames saved. ***")

		print("\t" + "\t" + "\t" + "*** Checking for missing matches... ***")
		#Check for unscrapped matches
		missing_matches = np.where(money_lines["ID"] == 0)[0]
		if len(missing_matches) == 0:
			sys.exit("\t" + "\t" + "No missing Box Scores.")

		missing_matches = money_lines.iloc[missing_matches, :].copy().reset_index(drop = True)
		rows = []	

		url_front = "https://www.fangraphs.com/boxscore.aspx?date="

		to_do = len(missing_matches)
		print("Extracting URLS and IDs for:" + "\t" + str(to_do) + " matches.")

		for i in tqdm(range(0, to_do)):

			to_plug = self.Translate_Team_Names(missing_matches.at[i, "Team_Home"], "FanGraphs")
			date = missing_matches.at[i, "Date"]

			url = url_front + date + "&team=" + to_plug + "&dh=0&season=" + date.split("-")[0]

			home_team = missing_matches.at[i, "Team_Home"]
			away_team = missing_matches.at[i, "Team_Away"]

			ID = ""
			temp = date.split("-")
			for values in temp:
				ID = ID + values

			ID = ID + numerise_string(home_team[0:2] + away_team[0:2])

			rows.append([url, ID])


		missing_matches = pd.DataFrame(data = rows, columns = ["URL", "ID"])


		doubles = [x for x in missing_matches["ID"] if len(np.where(missing_matches["ID"] == x)[0]) > 1]
		if len(doubles) > 0:

			fix = list(set(list(doubles)))
			for value in fix:

				index = np.where(missing_matches["ID"] == value)[0][0]

				missing_matches.at[index, "ID"] = "-" + str(missing_matches.at[index, "ID"])
				missing_matches.at[index, "URL"] = str(missing_matches.at[index, "URL"].replace("dh=0", "dh=1"))

		missing_matches["ID"] = missing_matches["ID"].astype("int64")

		path_save = self.paths[4] + "/Missing_Matches.csv"
		missing_matches.to_csv(path_save, index = False)
		


#####################################################
#################### EXAMPLE  #######################
#####################################################


file_path = "D:/MLB"
n = 7
self = Baseball_Scrapper(file_path)

#Update
self.UPDATE_FanGraphs_Box_Scores()

#Clean
self.Clean_Data()
self.Clean_Betting_Data()

#Build player database
self.Build_Individual_Players_Database()
self.Build_Individual_Players_ROLLING_AVERAGE_Database(n)

#Build regression frame
self.Prepare_Regression_Frames(n)



