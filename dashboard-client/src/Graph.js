import React, {Component} from 'react';
import { Container,Grid } from '@material-ui/core';
import {

BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Label, Text
} from 'recharts';
const BarComponent = (length) => {length ? <div><Tooltip /><Bar dataKey="count" fill="#1f416f"/></div>: null}
class Graph extends Component{
constructor(props){
super(props)
console.log("constructor")
}
componentDidUpdate(prevProps){

if(this.props.data !== prevProps.data){
console.log("updated")
}
}
shouldComponentUpdate(nextProps){
//const allEqual = arr => arr.every( val => val !== nextProps.data[0])
  //              if(allEqual(this.props.data)){
console.log(nextProps.data.data.length)
if(nextProps.data.data.length){
	console.log("will update")	
//console.log(nextProps.data)
return true
}
return false

}

render(){

return (<div className="graphContainer"><BarChart
        width={500}
        height={300}
        data={this.props.data.data.length?this.props.data.data:[{"word":"meh", "count":1}]}
        margin={{
          top: 5, right: 30, left: 20, bottom: 5,
        }}
      ><CartesianGrid strokeDasharray="3 3" horizontal={false} vertical={false} />
<XAxis interval={0} dataKey={this.props.data.xKey.toString()} tick={this.props.data.tick == "true"}/>
        <YAxis allowDecimals={false} label={<Text
      x={0}
      y={0}
      dx={50}
      dy={150}
      offset={0}
      angle={-90}
   >Count</Text>}/>
{this.props.data.data.length ? <Tooltip/>:null}
{this.props.data.data.length ? <Bar dataKey={this.props.data.yKey.toString()} fill="#1f416f"/>:null}

      </BarChart></div>
)}


}

export default Graph;
