import React, {Component} from 'react';
import { Container,Grid } from '@material-ui/core';
import Graph from './Graph';
import {XAxis, Label} from 'recharts';
const ws = new WebSocket('ws://10.0.2.2:8080/websocketserver-0.0.1-SNAPSHOT/data/10005');
const dataMap = {};
var currentMinuteMap = {};
const XAxisComponent = (tick, dataKey, label) => {return <XAxis interval={0} tick={tick} dataKey={dataKey}><Label value={label} offset={0}/></XAxis>};

class App extends Component{
	constructor(props){
		super(props)
		console.log('app constructor')
		this.state = {
			message:"",
			freqArray:[],
			minutesArray:[],
			noOfMessages: 0,
			currentTimeInterval: new Date()
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
				minutesArray:prevState.minutesArray.concat({"timestamp":prevState.currentTimeInterval.getUTCHours().toString() + ":" + prevState.currentTimeInterval.getUTCMinutes().toString(), "word":mostFreqWord, "count":maxVal})
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
	var array = []
	console.log("num of messages")
	console.log(this.state.noOfMessages)
	if(this.state.noOfMessages % 8 == 0){

		//Doesnt need to be done if only updating every 8 words
		//this is the bottleneck for loop. Quickly becomes too big
		//Put keyvalue pairs into an array for graph
		for(const [key, value] of Object.entries(dataMap)){
			array = array.concat({"word":key,"count":value})
		}
		var t2 = performance.now()
//		console.log(t2-t1)
	//	function compare (a,b){if(a.count>b.count){return -1}if(a.count<a.count){return 1}return 0}
		//Put the high counts first

		console.log("Num of words")
		console.log(array.length)
		array.sort((a,b) => a.count < b.count)
		//Shorten the data sent to graph
		if(array.length>30){array=array.slice(0,30)}
}
		var t3 = performance.now()
//			console.log(t3-t2)

			this.setState({freqArray:array,message: message.content, noOfMessages: this.state.noOfMessages + 1})
//		}

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
	return <Container>
		<h1 className="title-name">Twitter Feed Dashboard</h1>
		<Grid
  container
  direction="row"
  justify="center"
  alignItems="center"
  spacing={3}
>
<Grid item xs={6}>
<Graph data={this.state.freqArray} XAxis={XAxisComponent(false,"word","Frequent Words (total)")} noOfMessages={this.state.noOfMessages}></Graph>
</Grid>
<Grid item xs={6}>
<Graph data={this.state.minutesArray} XAxis={XAxisComponent(true, "timestamp","Frequent Words (per min)")} noOfMessages={this.state.noOfMessages}></Graph>
</Grid>
</Grid>
</Container>;
}
}

export default App;
