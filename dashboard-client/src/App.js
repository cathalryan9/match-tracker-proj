import React, {Component} from 'react';
import { Container,Grid } from '@material-ui/core';
import Graph from './Graph';
import {XAxis, Label} from 'recharts';
const ws = new WebSocket('ws://10.0.2.2:8080/websocketserver-0.0.1-SNAPSHOT/data/10005');
const dataMap = {};
var currentMinuteMap = {};

class App extends Component{
	constructor(props){
		super(props)
		this.state = {
			message:"",
			freqData:{"graphTitle":"Total Word Count (Top 30)","yKey":"count","xKey":"word","tick": "false", "data":[]},
			minutesData:{"graphTitle":"Top Word In Interval (1 Minute)","yKey":"count","xKey":"timestamp", "tick": "true", "data":[]},
			hashtagData:{"graphTitle":"Hashtag Count (Top 30)","yKey":"count","xKey":"word","tick":"false", "data":[]},
			noOfMessages: 0,
			currentTimeInterval: new Date(),
			updateInterval:10
			}
	}

	tick(){
		var maxVal = 0
		var mostFreqWord = ""
		for(const [key, value] of Object.entries(currentMinuteMap)){
			if (value > maxVal){
				maxVal = value
				mostFreqWord = key
			}
		currentMinuteMap = {}
        	}

		this.setState(prevState => ({currentTimeInterval:new Date(prevState.currentTimeInterval.getTime() + 60000),
					minutesData:{"graphTitle":"Top Word In Interval (1 Minute)","xKey":"timestamp", "yKey":"count", "tick":"true", 
						"data":prevState.minutesData.data.concat({"timestamp":prevState.currentTimeInterval.getUTCHours().toString() + ":" 
							+ prevState.currentTimeInterval.getUTCMinutes().toString(), "word":mostFreqWord, "count":maxVal})},
					updateInterval: Math.round(prevState.noOfMessages/50)
				 }))
	}

	componentDidMount() {
		this.interval = setInterval(() => this.tick(),60000)
	        ws.onopen = () => {
        	// on connecting, do nothing but log it to the console
	        console.log('connected')
        	}
ws.onmessage = evt => {
        // listen to data sent from the websocket server
	var t0 = performance.now()
        const message = JSON.parse(evt.data)
	const jsonMessage = JSON.parse(message.content)
	const word = jsonMessage["text"]

	if(!(word in dataMap)){
		dataMap[word] = 1
		currentMinuteMap[word] = 1
	}else if(!(word in currentMinuteMap)){
		currentMinuteMap[word] = 1
		dataMap[word] = dataMap[word] + 1
	}
	else{
		currentMinuteMap[word] = currentMinuteMap[word] + 1
		dataMap[word] = dataMap[word]+1
	}

	var t1 = performance.now()
//	console.log(t1-t0)
	var wordArray = []
	var hashtagArray = []
	if(this.state.noOfMessages % this.state.updateInterval == 0){

		//Doesnt need to be done if only updating every 10 words
		//this is the bottleneck for loop. Quickly becomes too big
		//Put keyvalue pairs into an array for graph
		for(const [key, value] of Object.entries(dataMap)){
			if(key.startsWith("#")){
				hashtagArray = hashtagArray.concat({"word":key,"count":value})
			}
			else{
				wordArray = wordArray.concat({"word":key,"count":value})
			}
		}
		var t2 = performance.now()
//		console.log(t2-t1)

		console.log("Num of words")
		console.log(wordArray.length)
		wordArray.sort((a,b) => a.count < b.count)
		hashtagArray.sort((a,b) => a.count < b.count)
		//Shorten the data sent to graph
		
		if(wordArray.length>30){wordArray=wordArray.slice(0,30)}
		if(hashtagArray.length>30){hashtagArray=hashtagArray.slice(0,30)}
}		
		var t3 = performance.now()
//			console.log(t3-t2)


			this.setState({
				freqData:{"graphTitle":"Total Word Count (Top 30)","yKey":"count","xKey": "word", "tick": "false", "data":wordArray},
				hashtagData:{"graphTitle":"Hashtag Count (Top 30)","yKey":"count","xKey":"word","tick":"false", "data":hashtagArray},
				message: message.content, noOfMessages: this.state.noOfMessages + 1
			})
        }

        ws.onclose = () => {
        console.log('disconnected')
        // automatically try to reconnect on connection loss

        }
	ws.onerror = () => {
	console.log('error with ws')
	}
}
	render(){
	return <Container className="app-container">
		<h1 className="title-name">Twitter Feed Dashboard</h1>
		<Grid
  container
  direction="row"
  justify="center"
  alignItems="center"
  spacing={1}
>
<Grid item xs={6} >
<Graph data={this.state.freqData} noOfMessages={this.state.noOfMessages}></Graph>
</Grid>
<Grid item xs={6} >
<Graph data={this.state.minutesData} noOfMessages={this.state.noOfMessages}></Graph>
</Grid>
<Grid item xs={6}>
<h2>Total words: {this.state.noOfMessages}</h2>
</Grid>
<Grid item xs={6}>
<Graph data={this.state.hashtagData} noOfMessages={this.state.noOfMessages}></Graph>
</Grid>
</Grid>
</Container>;
}
}

export default App;
