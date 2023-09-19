import weaviate
import time
import streamlit as st
class WeaviateHandler:
	def __init__(self,weaviatekey=st.secrets["WEAVIATE_KEY"],openaikey=st.secrets["OPENAI_KEY"]):
		self.auth_config = weaviate.AuthApiKey(api_key=weaviatekey)
		self.client = weaviate.Client(
			url="https://questionsdatabase-lzn96801.weaviate.network",
			auth_client_secret=self.auth_config,
			additional_headers={
				"X-OpenAI-Api-Key": openaikey,
			}
		)
	def get_questions(self,data):

		questions={
			"skills":[],
			"work_experience":[],
			"projects":[],
			"certificates":[],
		}
		answers={
			"skills":[],
			"work_experience":[],
			"projects":[],
			"certificates":[],
		}
		company={
			"skills":[],
			"work_experience":[],
			"projects":[],
			"certificates":[],
		}
		role={
			"skills":[],
			"work_experience":[],
			"projects":[],
			"certificates":[],
		}
		question_topics=set()

		if "work_experience" in data:
			with st.spinner("Fetching questions based on your work experience..."):
				for i in data["work_experience"]:
					concept=""
					if "job_title" in i:
						concept+=i["job_title"]
					if concept not in question_topics:
						response = (
							self.client.query
							.get("Questionnew", ["question", "answer","company","role"])
							.with_near_text({"concepts": concept})
							.with_limit(1)
						)
						response=response.do()
						if response:
							st.write(response)
							for k in response["data"]["Get"]["Questionnew"]:
								questions["work_experience"].append(k["question"])
								answers["work_experience"].append(k["answer"])
								company["work_experience"].append(k["company"])
								role["work_experience"].append(k["role"])
						question_topics.add(concept)
						time.sleep(20)

		if "projects" in data:
			with st.spinner("Fetching questions based on your projects..."):
				for i in data["projects"]:
					if "technologies_used" in i:
						for concept in i["technologies_used"]:
							if concept not in question_topics:
								response = (
									self.client.query
									.get("Questionnew", ["question", "answer","company","role"])
									.with_near_text({"concepts": concept})
									.with_limit(1)
								)
								response=response.do()
								if response:
									for k in response["data"]["Get"]["Questionnew"]:
										questions["projects"].append(k["question"])
										answers["projects"].append(k["answer"])
										company["projects"].append(k["company"])
										role["projects"].append(k["role"])
								question_topics.add(concept)
								time.sleep(20)
					
		if "certificates" in data:
			with st.spinner("Fetching questions based on your certificates..."):
				for i in data["certificates"]:
					concept=""
					if "name" in i:
						concept+=i["name"]
					if concept not in question_topics:
						response = (
							self.client.query
							.get("Questionnew", ["question", "answer","company","role"])
							.with_near_text({"concepts": concept})
							.with_limit(1)

						)
						response=response.do()
						if response:
							for k in response["data"]["Get"]["Questionnew"]:
								questions["certificates"].append(k["question"])
								answers["certificates"].append(k["answer"])
								company["certificates"].append(k["company"])
								role["certificates"].append(k["role"])
						question_topics.add(concept)
						time.sleep(20)

		if "skills" in data:
			with st.spinner("Fetching questions based on your skills..."):
				for i in data["skills"]:
					if i not in question_topics:
						response = (
							self.client.query
							.get("Questionnew", ["question", "answer","company","role"])
							.with_near_text({"concepts": i})
							.with_limit(1)

						)
						response=response.do()
						if response:
							for k in response["data"]["Get"]["Questionnew"]:
								questions["skills"].append(k["question"])
								answers["skills"].append(k["answer"])
								company["skills"].append(k["company"])
								role["skills"].append(k["role"])
						question_topics.add(i)
						time.sleep(20)

		return questions,answers,company,role
			
