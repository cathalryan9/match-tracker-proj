import React, {Component} from 'react';
import { Container,Grid } from '@material-ui/core';
import {
BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Label, Text
} from 'recharts';

const CustomTooltip = ({active,payload,label}) => {
  if (active) {
  return (<div className="tooltip-container"><p>{payload[0].payload.text}</p><p>{payload[0].payload.count}</p></div>);
 }
 return null 
}
const BarComponent = (length) => {length ? <div><Tooltip/><Bar dataKey="count" fill="#1f416f"/></div>: null}
class Graph extends Component{

constructor(props){
	super(props)
}
shouldComponentUpdate(nextProps){
		if(this.props.data !== nextProps.data){
		return true
        }
return false

}

render(){

return (<div className="graph-box">
	<Grid container direction="column" alignItems="center" spacing={0}>
	<Grid item className="graph-title-bar">{this.props.data.graphTitle}</Grid>
	<Grid item className="graph-container"><BarChart
        width={600}
        height={300}
        data={this.props.data.data.length?this.props.data.data:[{"word":"", "count":0}]}
        margin={{
          top: 5, right: 30, left: 20, bottom: 5,
        }}
      ><CartesianGrid strokeDasharray="3 3" horizontal={false} vertical={false} />
	<XAxis interval={0} dataKey={this.props.data.xKey.toString()} tick={this.props.data.tick == "true"} interval={this.props.data.data.length < 10 ? 0 : this.props.data.data.length < 45? 5: 10}/>
        <YAxis allowDecimals={false} label={<Text
      x={0}
      y={0}
      dx={50}
      dy={150}
      offset={0}
      angle={-90}
   >Count</Text>}/>
{this.props.data.data.length ? <Tooltip content={<CustomTooltip/>}/>:null}
{this.props.data.data.length ? <Bar dataKey={this.props.data.yKey.toString()} fill="#1f416f"/>:null}

      </BarChart></Grid></Grid></div>
)}


}

export default Graph;
