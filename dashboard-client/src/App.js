import React, {Component} from 'react';
import { Container,Grid } from '@material-ui/core';
import Graph from './Graph';
import Metrics from './Metrics';
import {XAxis, Label} from 'recharts';
const wordCountws = new WebSocket('ws://10.0.2.2:8080/websocketserver-0.0.1-SNAPSHOT/word_count/10005');
const hashtagCountws = new WebSocket('ws://10.0.2.2:8080/websocketserver-0.0.1-SNAPSHOT/hashtag_count/10005');

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
			updateInterval:20,
			messageRate:400
			}
	}


	componentDidMount() {
	        wordCountws.onopen = () => {
		        console.log('connected')
        	}
		wordCountws.onmessage = evt => {
		        // listen to data sent from the websocket server
        		const message = JSON.parse(evt.data)
			const jsonMessage = JSON.parse(message.content)
			var word = jsonMessage["text"]
			this.setState({freqData:{"graphTitle":"Total Word Count (Top 30)","yKey":"count","xKey":"word","tick": "false", "data":jsonMessage}})
	        }
		wordCountws.onerror = () => {
			console.log('error with ws')
		}

                hashtagCountws.onopen = () => {
                        console.log('connected')
                }
                hashtagCountws.onmessage = evt => {
                        // listen to data sent from the websocket server
                        const message = JSON.parse(evt.data)
                        const jsonMessage = JSON.parse(message.content)
                        var word = jsonMessage["text"]
                        this.setState({hashtagData:{"graphTitle":"Hashtag unt (Top 30)","yKey":"count","xKey":"word","tick":"false", "data":jsonMessage}})
                }
                hashtagCountws.onerror = () => {
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
<Metrics data={this.state.noOfMessages}></Metrics>
</Grid>
<Grid item xs={6}>
<Graph data={this.state.hashtagData} noOfMessages={this.state.noOfMessages}></Graph>
</Grid>
</Grid>
</Container>
}
}

export default App;
